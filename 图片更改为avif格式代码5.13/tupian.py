from __future__ import annotations

import shutil
from pathlib import Path
from typing import Tuple, Optional
from tkinter import Tk, simpledialog, messagebox

from PIL import Image

# 尝试导入avif支持
try:
    import pillow_avif_plugin

    AVIF_SUPPORT = True
except ImportError:
    AVIF_SUPPORT = False
    print("警告: AVIF格式需要 pillow-avif-plugin")


class ImageFilterPipeline:
    def __init__(
            self,
            input_base: str = "data",
            output_base: str = "data_output",
            final_size: Tuple[int, int] = (500, 500),
            fill_color: Tuple[int, int, int] = (255, 255, 255),
            max_final_count: int = 40,
            output_format: str = "avif",
            quality: int = 85,
            max_size_kb: int = 70,
    ):
        self.input_base = Path(input_base)
        self.output_base = Path(output_base)
        self.final_size = final_size
        self.fill_color = fill_color
        self.max_final_count = max_final_count
        self.output_format = output_format.lower()
        self.quality = quality
        self.max_size_kb = max_size_kb

        # 汇总统计
        self.total_images = 0
        self.success = 0
        self.failed = 0

    def get_product_name(self, keyword_name: str) -> str:
        """弹窗输入产品名（每个keyword弹窗一次）"""
        root = Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        while True:
            name = simpledialog.askstring(
                "输入产品名",
                f"正在处理文件夹: {keyword_name}\n\n请输入产品名（该文件夹下所有图片将使用这个名称）：",
                parent=root
            )
            if name is None:
                root.destroy()
                exit(0)
            name = name.strip()
            if name:
                root.destroy()
                return name
            messagebox.showerror("错误", "产品名不能为空")

    def convert_image(self, img_path: Path, output_path: Path) -> bool:
        """转换图片：缩放 + 格式转换 + 压缩到指定大小"""
        try:
            with Image.open(img_path) as img:
                # 转RGB
                if img.mode in ('RGBA', 'P'):
                    bg = Image.new('RGB', img.size, self.fill_color)
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    bg.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = bg
                elif img.mode != 'RGB':
                    img = img.convert('RGB')

                # 缩放到500x500（拉伸）
                img = img.resize(self.final_size, Image.Resampling.LANCZOS)

                # 动态调整质量，确保文件不超过 max_size_kb
                quality = self.quality
                min_quality = 20

                while min_quality <= quality:
                    # 尝试保存
                    if self.output_format == 'avif':
                        img.save(output_path, 'AVIF', quality=quality)
                    elif self.output_format == 'jpg':
                        img.save(output_path, 'JPEG', quality=quality)
                    elif self.output_format == 'png':
                        img.save(output_path, 'PNG')
                    elif self.output_format == 'webp':
                        img.save(output_path, 'WEBP', quality=quality)

                    # 检查文件大小
                    file_size_kb = output_path.stat().st_size / 1024

                    if file_size_kb <= self.max_size_kb:
                        # 文件大小符合要求
                        return True
                    else:
                        # 文件太大，降低质量
                        quality -= 10
                        if quality < min_quality:
                            # 降到最低质量还是太大，接受当前结果
                            return True

                return True

        except Exception as e:
            print(f"  转换失败 {img_path.name}: {e}")
            return False

    def process_oem(self, oem_path: Path, output_path: Path, product_name: str) -> dict:
        """处理单个OEM文件夹"""
        oem_name = oem_path.name
        stats = {"total": 0, "success": 0, "failed": 0}

        exts = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif'}
        images = [f for f in oem_path.iterdir() if f.is_file() and f.suffix.lower() in exts]
        stats["total"] = len(images)

        if not images:
            return stats

        print(f"\n  处理 OEM: {oem_name} (共{len(images)}张)")

        # 数量截断
        if len(images) > self.max_final_count:
            print(f"  截断: {len(images)} → {self.max_final_count}张")
            images = images[:self.max_final_count]

        # 创建输出目录
        if output_path.exists():
            shutil.rmtree(output_path)
        output_path.mkdir(parents=True)

        # 转换图片
        success_count = 0
        for idx, img_path in enumerate(images, 1):
            new_name = f"{product_name}-{oem_name}-{idx}.{self.output_format}"
            dest = output_path / new_name
            if self.convert_image(img_path, dest):
                success_count += 1
                file_size = dest.stat().st_size / 1024
                status = "✓" if file_size <= self.max_size_kb else f"⚠ {file_size:.0f}KB(超{self.max_size_kb}KB)"
                print(f"    [{status}] {img_path.name} → {new_name} ({file_size:.1f}KB)")
            else:
                print(f"    [✗] {img_path.name} 转换失败")

        stats["success"] = success_count
        stats["failed"] = stats["total"] - success_count
        print(f"  完成: 成功{success_count}张, 失败{stats['failed']}张")

        return stats

    def print_summary(self):
        """打印汇总统计"""
        print(f"\n{'=' * 60}")
        print("汇总统计")
        print(f"{'=' * 60}")
        print(f"  扫描图片总数:   {self.total_images}")
        print(f"  成功输出:       {self.success}")
        print(f"  失败:           {self.failed}")

        if self.total_images > 0:
            success_rate = self.success / self.total_images * 100
            print(f"\n  成功率:         {success_rate:.1f}%")
        print(f"{'=' * 60}")

    def run(self):
        """运行"""
        for keyword_dir in self.input_base.iterdir():
            if not keyword_dir.is_dir():
                continue

            print(f"\n{'=' * 50}")
            print(f"处理: {keyword_dir.name}")
            print(f"{'=' * 50}")

            product_name = self.get_product_name(keyword_dir.name)
            print(f"产品名: {product_name}")
            print(f"目标大小: ≤{self.max_size_kb}KB\n")

            sub_dirs = [d for d in keyword_dir.iterdir() if d.is_dir()]
            output_keyword_dir = self.output_base / keyword_dir.name

            if sub_dirs:
                for oem_dir in sub_dirs:
                    output_oem_dir = output_keyword_dir / oem_dir.name
                    stats = self.process_oem(oem_dir, output_oem_dir, product_name)
                    self.total_images += stats["total"]
                    self.success += stats["success"]
                    self.failed += stats["failed"]
            else:
                output_oem_dir = output_keyword_dir / keyword_dir.name
                stats = self.process_oem(keyword_dir, output_oem_dir, product_name)
                self.total_images += stats["total"]
                self.success += stats["success"]
                self.failed += stats["failed"]

        self.print_summary()


def main():
    pipeline = ImageFilterPipeline(
        input_base="data",
        output_base="data_output",
        final_size=(500, 500),
        output_format="avif",
        quality=85,
        max_size_kb=70,  # 最大70KB
        max_final_count=40,
    )
    pipeline.run()


if __name__ == "__main__":
    main()