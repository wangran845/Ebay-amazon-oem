from __future__ import annotations

import pandas as pd
import os
import shutil
from pathlib import Path
from data.config import Config


class ImagePathUpdater:
    """更新图片路径、删除空文件夹、删除无图片行"""

    # 支持的图片格式
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}

    def __init__(self, config: Config = None, base_path: str = r"E:\ome_picture"): #原路径在E盘
        self.config = config if config else Config()
        self.product_name = self.config.product_user_2
        self.base_path = Path(base_path)

        # 构建完整的基础路径: E:\ome_picture\product_name
        self.full_base_path = self.base_path / self.product_name

        # 输入输出路径
        self.src_file = Path(f"data/{self.product_name}/{self.product_name}_HTML格式化.xlsx")
        self.dst_file = Path(f"data/{self.product_name}/{self.product_name}_最终.xlsx")

    def get_images_and_clean_folder(self, oem: str) -> tuple[str, bool]:
        """
        扫描OEM文件夹，返回图片路径，并删除空文件夹
        返回: (图片路径字符串, 是否删除了文件夹)
        """
        if pd.isna(oem) or str(oem).strip() == '':
            return '', False

        oem = str(oem).strip()
        folder_path = self.full_base_path / oem

        # 如果文件夹不存在，返回空
        if not folder_path.exists():
            return '', False

        # 获取所有图片文件
        image_files = set()
        for ext in self.IMAGE_EXTENSIONS:
            for file_path in folder_path.glob(f"*{ext}"):
                image_files.add(file_path)

        # 如果没有图片，删除文件夹
        if not image_files:
            try:
                shutil.rmtree(folder_path)
                return '', True
            except Exception as e:
                print(f"      ⚠️  删除文件夹失败 {folder_path}: {e}")
                return '', False

        # 排序并构建相对路径
        image_files = sorted(image_files, key=lambda x: x.name)
        relative_paths = [
            f"ome_picture/{self.product_name}/{oem}/{img.name}"
            for img in image_files
        ]

        return ';'.join(relative_paths), False

    def run(self) -> None:
        """主处理流程"""
        print(f"===== 图片路径更新与清理 =====")
        print(f"[1/4] 读取源文件: {self.src_file}")

        if not self.src_file.exists():
            print(f"❌ 错误: 文件不存在 - {self.src_file}")
            return

        df = pd.read_excel(self.src_file)
        original_count = len(df)
        print(f"      成功读取 {original_count} 行数据")

        # 查找图片路径列
        image_col = None
        for col in ['image_paths', '图片本地地址', 'images', 'picture']:
            if col in df.columns:
                image_col = col
                break

        if not image_col:
            print(f"❌ 错误: 未找到图片路径列，可用列: {list(df.columns)}")
            return

        print(f"[2/4] 扫描图片列 '{image_col}' 并清理空文件夹...")

        # 获取唯一的OEM号列表
        oem_col = None
        for col in ['OEM', 'OME', 'oem', 'ome']:
            if col in df.columns:
                oem_col = col
                break

        if not oem_col:
            print(f"❌ 错误: 未找到OEM列，可用列: {list(df.columns)}")
            return

        # 更新图片路径并清理空文件夹
        deleted_folders = 0
        new_image_paths = []

        for idx, row in df.iterrows():
            oem = row[oem_col]
            image_path, was_deleted = self.get_images_and_clean_folder(oem)
            new_image_paths.append(image_path)
            if was_deleted:
                deleted_folders += 1

        df[image_col] = new_image_paths
        print(f"      扫描完成，删除 {deleted_folders} 个空文件夹")

        # 统计更新前
        rows_with_images_before = df[image_col].astype(bool).sum()
        print(f"[3/4] 更新前有效图片行: {rows_with_images_before}/{original_count}")

        # 删除图片路径为空的行
        print(f"[4/4] 删除图片路径为空的行...")
        df = df[df[image_col].astype(bool)].reset_index(drop=True)
        deleted_rows = original_count - len(df)

        print(f"      删除 {deleted_rows} 行，剩余 {len(df)} 行")

        # 保存
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)
        print(f"\n✅ 处理完成，已保存到: {self.dst_file}")


def main():
    """供 main(1).py 调用的入口"""
    updater = ImagePathUpdater()
    updater.run()


if __name__ == "__main__":
    main()