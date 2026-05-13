from __future__ import annotations

import os
import shutil
import re
from pathlib import Path
from typing import List, Tuple, Optional, Dict
import numpy as np

from PIL import Image, ImageOps

# 尝试导入avif支持（Pillow需要编译时支持，或使用pillow-avif-plugin）
try:
    from pillow_avif import AvifImagePlugin

    AVIF_SUPPORT = True
except ImportError:
    AVIF_SUPPORT = False
    print("警告: pillow-avif-plugin未安装，AVIF转换功能将不可用。安装命令: pip install pillow-avif-plugin")


class PHash:
    """纯numpy实现的pHash算法"""

    @staticmethod
    def _dct1d(vector: np.ndarray) -> np.ndarray:
        """一维DCT变换"""
        n = len(vector)
        result = np.zeros_like(vector, dtype=np.float32)
        for k in range(n):
            sum_val = 0.0
            for i in range(n):
                sum_val += vector[i] * np.cos(np.pi * k * (2 * i + 1) / (2 * n))
            if k == 0:
                result[k] = sum_val * np.sqrt(1 / n)
            else:
                result[k] = sum_val * np.sqrt(2 / n)
        return result

    @staticmethod
    def _dct2d(matrix: np.ndarray) -> np.ndarray:
        """二维DCT变换"""
        tmp = np.apply_along_axis(PHash._dct1d, 0, matrix)
        return np.apply_along_axis(PHash._dct1d, 1, tmp)

    @staticmethod
    def calculate(img: Image.Image) -> str | None:
        """计算pHash"""
        try:
            if img.mode != 'L':
                img = img.convert('L')
            img = img.resize((32, 32), Image.Resampling.LANCZOS)
            pixels = np.array(img, dtype=np.float32)
            dct = PHash._dct2d(pixels)
            dct_low = dct[:8, :8]
            avg = (dct_low.sum() - dct_low[0, 0]) / 63
            hash_bits = (dct_low > avg).flatten()
            hex_str = ''
            for i in range(0, 64, 4):
                nibble = sum(bit << (3 - j) for j, bit in enumerate(hash_bits[i:i + 4]))
                hex_str += format(nibble, 'x')
            return hex_str
        except Exception as e:
            print(f"  [pHash错误]: {e}")
            return None

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        """计算汉明距离"""
        if len(hash1) != 16 or len(hash2) != 16:
            return 64
        distance = 0
        for c1, c2 in zip(hash1, hash2):
            x = int(c1, 16) ^ int(c2, 16)
            distance += bin(x).count('1')
        return distance


class ImageFilterPipeline:
    """
    图像筛选管道：
    尺寸标准化 → 白边检测 → pHash相似度去重 → 数量截断 → 格式转换AVIF → 重命名
    """

    def __init__(
            self,
            input_base: str = "data",
            output_base: str = "data_output",
            standard_size: Tuple[int, int] = (800, 800),
            final_size: Tuple[int, int] = (500, 500),
            keep_aspect: bool = True,
            fill_color: Tuple[int, int, int] = (255, 255, 255),
            white_threshold: int = 200,
            white_ratio: float = 0.7,
            boundary_width: int = 3,
            hash_threshold: float = 0.8,
            min_keep_count: int = 8,
            max_final_count: int = 40,
            output_format: str = "avif",
            avif_quality: int = 85,
            jpeg_quality: int = 95,
            rename_prefix: Optional[str] = None,  # 新增：重命名前缀
            preserve_original_name: bool = True,  # 新增：是否保留原文件名
            custom_name_map: Optional[Dict[str, str]] = None,  # 新增：自定义名称映射
    ):
        self.input_base = Path(input_base)
        self.output_base = Path(output_base)
        self.standard_size = standard_size
        self.final_size = final_size
        self.keep_aspect = keep_aspect
        self.fill_color = fill_color
        self.white_threshold = white_threshold
        self.white_ratio = white_ratio
        self.boundary_width = boundary_width
        self.hash_threshold = hash_threshold
        self.min_keep_count = min_keep_count
        self.max_final_count = max_final_count
        self.max_hash_distance = int(64 * (1 - hash_threshold))
        self.output_format = output_format.lower()
        self.avif_quality = avif_quality
        self.jpeg_quality = jpeg_quality

        # 重命名相关参数
        self.rename_prefix = rename_prefix
        self.preserve_original_name = preserve_original_name
        self.custom_name_map = custom_name_map or {}

        # 验证输出格式
        self._validate_output_format()

    def _validate_output_format(self):
        """验证输出格式是否支持"""
        supported_formats = ['jpg', 'jpeg', 'png', 'webp', 'avif']
        if self.output_format not in supported_formats:
            raise ValueError(f"不支持的输出格式: {self.output_format}，支持: {supported_formats}")

        if self.output_format == 'avif' and not AVIF_SUPPORT:
            raise ImportError("AVIF格式需要安装pillow-avif-plugin: pip install pillow-avif-plugin")

    def generate_output_name(self, original_path: Path, index: int, oem_name: str = None) -> str:
        """
        生成输出文件名

        命名规则：
        1. 如果提供了自定义映射，优先使用
        2. 如果设置了重命名前缀，格式：{prefix}{原文件名}.{ext} 或 {prefix}{index}.{ext}
        3. 否则使用默认格式：{oem_name}-{index}.{ext}

        Args:
            original_path: 原始文件路径
            index: 序号
            oem_name: OEM文件夹名称
        """
        ext_map = {
            'jpg': '.jpg',
            'jpeg': '.jpg',
            'png': '.png',
            'webp': '.webp',
            'avif': '.avif'
        }
        file_ext = ext_map.get(self.output_format, '.jpg')

        # 1. 检查自定义映射（基于原文件名）
        original_name = original_path.stem
        if original_name in self.custom_name_map:
            custom_name = self.custom_name_map[original_name]
            # 确保文件名安全（移除非法字符）
            custom_name = self._sanitize_filename(custom_name)
            return f"{custom_name}{file_ext}"

        # 2. 使用重命名前缀
        if self.rename_prefix:
            if self.preserve_original_name:
                # 清理原文件名中的特殊字符
                safe_name = self._sanitize_filename(original_name)
                return f"{self.rename_prefix}{safe_name}{file_ext}"
            else:
                return f"{self.rename_prefix}{index:03d}{file_ext}"

        # 3. 默认命名方式
        return f"{oem_name}-{index}{file_ext}"

    def _sanitize_filename(self, filename: str) -> str:
        """
        清理文件名，移除不安全字符
        """
        # 移除或替换Windows/Linux文件名中的非法字符
        unsafe_chars = r'[<>:"/\\|?*]'
        sanitized = re.sub(unsafe_chars, '_', filename)
        # 移除开头和结尾的空格、点号
        sanitized = sanitized.strip('. ')
        # 限制长度（避免过长）
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized if sanitized else "unnamed"

    def resize_with_smart_crop(self, img: Image.Image, target_size: Tuple[int, int]) -> Image.Image:
        """
        智能缩放到目标尺寸
        如果保持宽高比，会居中裁剪或填充
        """
        if not self.keep_aspect:
            return img.resize(target_size, Image.Resampling.LANCZOS)

        # 计算缩放比例
        target_w, target_h = target_size
        img_w, img_h = img.size

        ratio_w = target_w / img_w
        ratio_h = target_h / img_h

        # 选择缩放比例（覆盖整个目标区域）
        scale = max(ratio_w, ratio_h)
        new_w = int(img_w * scale)
        new_h = int(img_h * scale)

        # 缩放图片
        img_resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

        # 居中裁剪
        left = (new_w - target_w) // 2
        top = (new_h - target_h) // 2
        right = left + target_w
        bottom = top + target_h

        return img_resized.crop((left, top, right, bottom))

    def standardize_image(self, img_path: Path) -> Optional[Image.Image]:
        """标准化图片尺寸（第一阶段：统一到中间尺寸）"""
        try:
            with Image.open(img_path) as img:
                if img.mode in ('RGBA', 'P'):
                    background = Image.new('RGB', img.size, self.fill_color)
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')

                if not self.keep_aspect:
                    return img.resize(self.standard_size, Image.Resampling.LANCZOS)

                img.thumbnail(self.standard_size, Image.Resampling.LANCZOS)
                new_img = Image.new('RGB', self.standard_size, self.fill_color)
                x = (self.standard_size[0] - img.width) // 2
                y = (self.standard_size[1] - img.height) // 2
                new_img.paste(img, (x, y))
                return new_img
        except Exception as e:
            print(f"  [标准化错误] {img_path}: {e}")
            return None

    def convert_to_final_format(self, img: Image.Image, output_path: Path) -> bool:
        """
        转换图片到最终格式
        """
        try:
            # 先缩放到最终尺寸
            final_img = self.resize_with_smart_crop(img, self.final_size)

            # 根据输出格式保存
            if self.output_format in ['jpg', 'jpeg']:
                # JPEG不支持透明通道，确保是RGB模式
                if final_img.mode == 'RGBA':
                    bg = Image.new('RGB', final_img.size, self.fill_color)
                    bg.paste(final_img, mask=final_img.split()[-1])
                    final_img = bg
                elif final_img.mode != 'RGB':
                    final_img = final_img.convert('RGB')
                final_img.save(output_path, 'JPEG', quality=self.jpeg_quality, optimize=True)

            elif self.output_format == 'png':
                # PNG支持透明，但要考虑文件大小
                if final_img.mode not in ['RGBA', 'RGB']:
                    final_img = final_img.convert('RGB')
                final_img.save(output_path, 'PNG', optimize=True)

            elif self.output_format == 'webp':
                final_img.save(output_path, 'WEBP', quality=self.jpeg_quality, method=6)

            elif self.output_format == 'avif':
                # AVIF格式保存
                if AVIF_SUPPORT:
                    # 确保RGB模式
                    if final_img.mode == 'RGBA':
                        bg = Image.new('RGB', final_img.size, self.fill_color)
                        bg.paste(final_img, mask=final_img.split()[-1])
                        final_img = bg
                    elif final_img.mode != 'RGB':
                        final_img = final_img.convert('RGB')

                    # AVIF保存参数
                    final_img.save(
                        output_path,
                        'AVIF',
                        quality=self.avif_quality,
                        speed=8
                    )
                else:
                    print(f"    [错误] AVIF格式不支持，回退到JPEG")
                    return self.convert_to_final_format(img, output_path.with_suffix('.jpg'))

            return True

        except Exception as e:
            print(f"    [格式转换错误] {output_path}: {e}")
            return False

    def is_white_border(self, img: Image.Image) -> bool:
        """检测左右边界是否为白边"""
        try:
            width, height = img.size
            if width < self.boundary_width * 2 or height < 10:
                return False

            left_boundary = img.crop((0, 0, self.boundary_width, height))
            right_boundary = img.crop((width - self.boundary_width, 0, width, height))

            def calc_white_ratio(boundary_img: Image.Image) -> float:
                pixels = list(boundary_img.getdata())
                if not pixels:
                    return 0
                white_count = sum(
                    1 for r, g, b in pixels
                    if r > self.white_threshold and g > self.white_threshold and b > self.white_threshold
                )
                return white_count / len(pixels)

            return (calc_white_ratio(left_boundary) >= self.white_ratio and
                    calc_white_ratio(right_boundary) >= self.white_ratio)
        except Exception as e:
            print(f"  [边界检测错误]: {e}")
            return False

    def process_single_image(self, img_path: Path) -> Tuple[Optional[Image.Image], Optional[str], bool]:
        """处理单张图片"""
        std_img = self.standardize_image(img_path)
        if std_img is None:
            return None, None, False

        passed = self.is_white_border(std_img)
        hash_val = PHash.calculate(std_img) if passed else None
        return std_img, hash_val, passed

    def deduplicate_by_phash(self, image_data: List[Tuple[Path, Image.Image, str]]) -> List[Tuple[Path, Image.Image]]:
        """pHash去重"""
        if len(image_data) < self.min_keep_count:
            print(f"    [保底] 图片数量{len(image_data)} < {self.min_keep_count}，全部保留")
            return [(path, img) for path, img, _ in image_data]

        kept = [image_data[0]]

        for item in image_data[1:]:
            img_path, img, img_hash = item
            is_duplicate = False

            for _, _, kept_hash in kept:
                distance = PHash.hamming_distance(img_hash, kept_hash)
                if distance <= self.max_hash_distance:
                    is_duplicate = True
                    print(f"    [去重] {img_path.name} 与某图相似(距离{distance})")
                    break

            if not is_duplicate:
                kept.append(item)

        print(f"    [去重结果] {len(image_data)}张 → 保留{len(kept)}张")
        return [(path, img) for path, img, _ in kept]

    def limit_and_rename(self, images: List[Tuple[Path, Image.Image]], oem_name: str, output_oem_path: Path) -> int:
        """
        数量截断 + 格式转换 + 重命名（支持自定义前缀）
        """
        original_count = len(images)

        # 数量截断
        if len(images) > self.max_final_count:
            print(f"    [截断] {len(images)}张 > {self.max_final_count}张，只保留前{self.max_final_count}张")
            images = images[:self.max_final_count]

        # 清空并创建输出目录
        if output_oem_path.exists():
            shutil.rmtree(output_oem_path)
        output_oem_path.mkdir(parents=True, exist_ok=True)

        # 转换格式并保存
        success_count = 0
        rename_info = []  # 记录重命名信息用于输出

        for idx, (img_path, std_img) in enumerate(images, start=1):
            # 生成输出文件名
            output_name = self.generate_output_name(img_path, idx, oem_name)
            dest = output_oem_path / output_name

            if self.convert_to_final_format(std_img, dest):
                success_count += 1
                rename_info.append((img_path.name, output_name))

                # 打印转换信息
                if idx <= 3 or idx == len(images):
                    print(
                        f"    [转换] {img_path.name} → {output_name} ({self.final_size[0]}×{self.final_size[1]}, {self.output_format.upper()})")

        # 打印重命名汇总
        if self.rename_prefix and rename_info:
            print(f"\n    [重命名汇总] 前缀: '{self.rename_prefix}'")
            if self.preserve_original_name:
                print(f"    命名规则: {self.rename_prefix}原文件名")
            else:
                print(f"    命名规则: {self.rename_prefix}序号")
            print(f"    共重命名 {len(rename_info)} 个文件")

            # 可选：保存重命名映射到文件
            mapping_file = output_oem_path / "_rename_mapping.txt"
            with open(mapping_file, 'w', encoding='utf-8') as f:
                f.write(f"前缀: {self.rename_prefix}\n")
                f.write(f"规则: {'保留原文件名' if self.preserve_original_name else '序号命名'}\n")
                f.write("-" * 50 + "\n")
                for original, new in rename_info:
                    f.write(f"{original} -> {new}\n")

        print(f"    [完成] {original_count}张 → 成功转换{success_count}张")
        print(f"    输出格式: {self.output_format.upper()}, 尺寸: {self.final_size[0]}×{self.final_size[1]}")
        return success_count

    def process_oem_folder(self, oem_path: Path, output_oem_path: Path) -> dict:
        """处理单个OEM文件夹"""
        oem_name = oem_path.name
        stats = {"total": 0, "standardized": 0, "white_border": 0, "final": 0}

        print(f"\n处理: {oem_path}")

        image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif'}
        all_images = [
            f for f in oem_path.iterdir()
            if f.is_file() and f.suffix.lower() in image_extensions
        ]

        stats["total"] = len(all_images)
        if not all_images:
            print("  无图片文件")
            return stats

        print(f"  发现 {len(all_images)} 张图片")

        # 处理每张图片
        processed = []
        for img_path in all_images:
            std_img, hash_val, passed = self.process_single_image(img_path)

            if std_img is None:
                print(f"  [跳过-读取失败] {img_path.name}")
                continue

            stats["standardized"] += 1

            if passed:
                processed.append((img_path, std_img, hash_val))
            else:
                pass

        stats["white_border"] = len(processed)
        print(f"  白边筛选: {stats['standardized']} → {len(processed)}")

        if not processed:
            print("  无通过白边筛选的图片")
            return stats

        # pHash去重
        deduped_images = self.deduplicate_by_phash(processed)

        # 数量截断 + 格式转换 + 重命名
        final_count = self.limit_and_rename(deduped_images, oem_name, output_oem_path)
        stats["final"] = final_count

        print(f"  输出: {final_count}张 → {output_oem_path}")
        return stats

    def process_keyword(self, keyword: str) -> dict:
        """处理一个key_word下的所有OEM文件夹"""
        keyword_input = self.input_base / keyword
        keyword_output = self.output_base / keyword

        if not keyword_input.exists():
            print(f"输入目录不存在: {keyword_input}")
            return {}

        oem_folders = [d for d in keyword_input.iterdir() if d.is_dir()]

        print(f"\n{'=' * 60}")
        print(f"处理 key_word: {keyword}")
        print(f"发现 {len(oem_folders)} 个OEM文件夹")
        print(f"最终尺寸: {self.final_size[0]}×{self.final_size[1]}")
        print(f"最终格式: {self.output_format.upper()}")
        print(f"最大保留数: {self.max_final_count}张/OEM")
        if self.rename_prefix:
            print(f"重命名前缀: '{self.rename_prefix}'")
            print(f"保留原文件名: {self.preserve_original_name}")
        print(f"{'=' * 60}")

        total_stats = {"total": 0, "standardized": 0, "white_border": 0, "final": 0, "oem_count": 0}

        for oem_folder in sorted(oem_folders):
            output_oem_path = keyword_output / oem_folder.name
            stats = self.process_oem_folder(oem_folder, output_oem_path)

            for k in ["total", "standardized", "white_border", "final"]:
                total_stats[k] += stats.get(k, 0)
            if stats["total"] > 0:
                total_stats["oem_count"] += 1

        # 计算文件大小统计
        if keyword_output.exists():
            total_size = sum(f.stat().st_size for f in keyword_output.rglob('*') if f.is_file())
            avg_size = total_size / total_stats["final"] if total_stats["final"] > 0 else 0
            print(f"\n[{keyword}] 存储统计:")
            print(f"  总文件大小: {total_size / 1024 / 1024:.2f} MB")
            print(f"  平均文件大小: {avg_size / 1024:.2f} KB")

        print(f"\n[{keyword}] 汇总:")
        print(f"  处理OEM数: {total_stats['oem_count']}")
        print(
            f"  总图片: {total_stats['total']} → 标准化: {total_stats['standardized']} → 白边: {total_stats['white_border']} → 最终: {total_stats['final']}")

        return total_stats

    def run(self, keywords: List[str] | None = None) -> None:
        """运行完整流程"""
        if keywords is None:
            if not self.input_base.exists():
                raise FileNotFoundError(f"输入目录不存在: {self.input_base}")
            keywords = [d.name for d in self.input_base.iterdir() if d.is_dir()]

        print(f"开始处理，共 {len(keywords)} 个key_word")
        print(f"参数: 中间尺寸={self.standard_size}, 最终尺寸={self.final_size}")
        print(f"白边阈值={self.white_threshold}, 相似度={self.hash_threshold}")
        print(f"保底={self.min_keep_count}, 最大保留={self.max_final_count}")
        print(f"输出格式={self.output_format.upper()}, AVIF质量={self.avif_quality}")

        grand_total = {"total": 0, "standardized": 0, "white_border": 0, "final": 0}

        for keyword in keywords:
            stats = self.process_keyword(keyword)
            if stats:
                for k in grand_total:
                    grand_total[k] += stats.get(k, 0)

        print(f"\n{'=' * 60}")
        print("全部处理完成")
        print(
            f"总计: {grand_total['total']} → {grand_total['standardized']} → {grand_total['white_border']} → {grand_total['final']}")

        if grand_total['final'] > 0:
            print(f"总体保留率: {grand_total['final'] / grand_total['total'] * 100:.1f}%")
        print(f"{'=' * 60}")


def main():
    """使用示例"""

    # 示例1：使用前缀重命名，保留原文件名
    pipeline1 = ImageFilterPipeline(
        input_base="data",
        output_base="data_output",
        standard_size=(800, 800),
        final_size=(500, 500),
        keep_aspect=True,
        fill_color=(255, 255, 255),
        white_threshold=200,
        white_ratio=0.7,
        hash_threshold=0.7,
        min_keep_count=8,
        max_final_count=40,
        output_format="avif",
        avif_quality=85,
        jpeg_quality=95,
        rename_prefix="a-",  # 设置前缀为"a-"
        preserve_original_name=True  # 保留原文件名
    )
    pipeline1.run()

    # 示例2：使用前缀+序号重命名
    # pipeline2 = ImageFilterPipeline(
    #     input_base="data",
    #     output_base="data_output",
    #     rename_prefix="photo-",
    #     preserve_original_name=False  # 使用序号命名
    # )
    # pipeline2.run()

    # 示例3：自定义文件名映射（将特定文件重命名为指定名称）
    # custom_map = {
    #     "IMG_001.jpg": "cover",
    #     "IMG_002.jpg": "detail_1",
    #     "original_img.png": "main"
    # }
    # pipeline3 = ImageFilterPipeline(
    #     input_base="data",
    #     output_base="data_output",
    #     rename_prefix="a-",
    #     preserve_original_name=True,
    #     custom_name_map=custom_map  # 优先使用映射名称
    # )
    # pipeline3.run()

    # 示例4：不使用重命名（保持默认命名）
    # pipeline4 = ImageFilterPipeline(
    #     input_base="data",
    #     output_base="data_output",
    #     rename_prefix=None  # 不使用前缀
    # )
    # pipeline4.run()


if __name__ == "__main__":
    main()