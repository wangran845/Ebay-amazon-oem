from __future__ import annotations

import os
import shutil
import re
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np

from PIL import Image


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
    尺寸标准化 → 白边检测 → pHash相似度去重 → 数量截断(40张) → 重命名({OEM}-n.jpg)
    """

    def __init__(
            self,
            input_base: str = "data",
            output_base: str = "data_output",
            standard_size: Tuple[int, int] = (800, 800),
            keep_aspect: bool = True,
            fill_color: Tuple[int, int, int] = (255, 255, 255),
            white_threshold: int = 200,
            white_ratio: float = 0.7,
            boundary_width: int = 3,
            hash_threshold: float = 0.8,
            min_keep_count: int = 8,
            max_final_count: int = 40,  # 新增：最终保留最大数量
    ):
        self.input_base = Path(input_base)
        self.output_base = Path(output_base)
        self.standard_size = standard_size
        self.keep_aspect = keep_aspect
        self.fill_color = fill_color
        self.white_threshold = white_threshold
        self.white_ratio = white_ratio
        self.boundary_width = boundary_width
        self.hash_threshold = hash_threshold
        self.min_keep_count = min_keep_count
        self.max_final_count = max_final_count
        self.max_hash_distance = int(64 * (1 - hash_threshold))

    def standardize_image(self, img_path: Path) -> Optional[Image.Image]:
        """标准化图片尺寸"""
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
        数量截断 + 重命名
        - 超过40张只保留前40张
        - 重命名为 {OEM}-n.jpg
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

        # 重命名保存
        for idx, (img_path, std_img) in enumerate(images, start=1):
            new_name = f"{oem_name}-{idx}.jpg"
            dest = output_oem_path / new_name
            std_img.save(dest, 'JPEG', quality=95)

        print(
            f"    [重命名] {original_count}张 → 保留{len(images)}张，格式: {oem_name}-1.jpg ~ {oem_name}-{len(images)}.jpg")
        return len(images)

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
                n = 0

        stats["white_border"] = len(processed)
        print(f"  白边筛选: {stats['standardized']} → {len(processed)}")

        if not processed:
            print("  无通过白边筛选的图片")
            return stats

        # pHash去重
        deduped_images = self.deduplicate_by_phash(processed)

        # 数量截断 + 重命名（关键修改）
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
        print(f"最大保留数: {self.max_final_count}张/OEM")
        print(f"{'=' * 60}")

        total_stats = {"total": 0, "standardized": 0, "white_border": 0, "final": 0, "oem_count": 0}

        for oem_folder in sorted(oem_folders):
            output_oem_path = keyword_output / oem_folder.name
            stats = self.process_oem_folder(oem_folder, output_oem_path)

            for k in ["total", "standardized", "white_border", "final"]:
                total_stats[k] += stats.get(k, 0)
            if stats["total"] > 0:
                total_stats["oem_count"] += 1

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
        print(f"参数: 标准尺寸={self.standard_size}, 白边阈值={self.white_threshold}, "
              f"相似度={self.hash_threshold}, 保底={self.min_keep_count}, 最大保留={self.max_final_count}")

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
        print(f"{'=' * 60}")


def main():
    """使用示例"""
    pipeline = ImageFilterPipeline(
        input_base="data",
        output_base="data_output",
        standard_size=(800, 800),
        keep_aspect=True,
        fill_color=(255, 255, 255),
        white_threshold=200,
        white_ratio=0.7,
        hash_threshold=0.7,
        min_keep_count=8,
        max_final_count=40,  # 每个OEM最多保留40张
    )

    pipeline.run()


if __name__ == "__main__":
    main()
