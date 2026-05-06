from __future__ import annotations

import pandas as pd
import re
from pathlib import Path

from data.config import Config


class CircleNumberReplacer:
    """
    将指定列中的圈数字（①、②等）替换为普通数字（1、2等）
    同时支持修复编码乱码问题（如 в┘ → ① → 1）
    """

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_中间格式化.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_圈数字替换.xlsx")

        # 要处理的列名
        self.target_column = "产品评论"

        # 圈数字映射表（①-⑳）
        self.circle_to_num = {
            '①': '1', '②': '2', '③': '3', '④': '4', '⑤': '5',
            '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9', '⑩': '10',
            '⑪': '11', '⑫': '12', '⑬': '13', '⑭': '14', '⑮': '15',
            '⑯': '16', '⑰': '17', '⑱': '18', '⑲': '19', '⑳': '20'
        }

        # 乱码修复映射（latin1编码问题导致的）
        self.garbled_to_circle = {
            'в┘': '①', 'в┌': '②', 'в█': '③', 'в▄': '④',
            'в▌': '⑤', 'в▐': '⑥', 'в▀': '⑦', 'вр': '⑧',
            'вс': '⑨', 'вт': '⑩'
        }

    def _replace_circle_numbers(self, text) -> str:
        """将圈数字替换为普通数字"""
        if pd.isna(text):
            return text

        text = str(text)

        # 执行替换
        for circle, num in self.circle_to_num.items():
            text = text.replace(circle, num)

        return text

    def _fix_encoding_and_replace(self, text) -> str:
        """修复编码乱码并替换圈数字"""
        if pd.isna(text):
            return text

        text = str(text)

        # 第一步：修复编码乱码（如果有）
        for garbled, circle in self.garbled_to_circle.items():
            text = text.replace(garbled, circle)

        # 第二步：替换圈数字为普通数字
        for circle, num in self.circle_to_num.items():
            text = text.replace(circle, num)

        return text

    def run(self, fix_encoding: bool = True) -> None:
        """
        主流程

        Args:
            fix_encoding: 是否先修复编码乱码（默认True）
        """
        print(f"[CircleNumberReplacer] 读取: {self.src_file}")

        # 检查文件是否存在
        if not self.src_file.exists():
            print(f"❌ 错误: 文件不存在 {self.src_file}")
            return

        df = pd.read_excel(self.src_file)
        before_count = len(df)
        print(f"                    成功读取 {before_count} 行数据")

        # 检查目标列是否存在
        if self.target_column not in df.columns:
            print(f"⚠️  警告: 未找到列 '{self.target_column}'")
            print(f"   可用列: {list(df.columns)}")
            return

        # 统计包含圈数字的行数
        def has_circle_number(text):
            if pd.isna(text):
                return False
            text = str(text)
            return any(circle in text for circle in self.circle_to_num.keys())

        rows_with_circles = df[self.target_column].apply(has_circle_number).sum()
        print(f"[CircleNumberReplacer] 包含圈数字的行数: {rows_with_circles}")

        # 统计包含乱码的行数
        if fix_encoding:
            def has_garbled(text):
                if pd.isna(text):
                    return False
                text = str(text)
                return any(garbled in text for garbled in self.garbled_to_circle.keys())

            rows_with_garbled = df[self.target_column].apply(has_garbled).sum()
            if rows_with_garbled > 0:
                print(f"[CircleNumberReplacer] 包含编码乱码的行数: {rows_with_garbled}")
                print(f"                   将先修复乱码再替换圈数字")

        # 执行替换
        print("[CircleNumberReplacer] 正在处理...")

        if fix_encoding:
            df[self.target_column] = df[self.target_column].apply(self._fix_encoding_and_replace)
        else:
            df[self.target_column] = df[self.target_column].apply(self._replace_circle_numbers)

        # 统计替换后的变化
        rows_after = df[self.target_column].apply(has_circle_number).sum()
        print(f"[CircleNumberReplacer] 替换后仍包含圈数字的行数: {rows_after}")

        # 保存文件
        print("[CircleNumberReplacer] 保存文件...")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)

        print(f"\n✅ [CircleNumberReplacer] 已保存到: {self.dst_file.resolve()}")

        # 显示样例对比
        self._show_sample(df)

    def _show_sample(self, df: pd.DataFrame) -> None:
        """显示处理前后的样例对比"""
        print("\n📊 处理样例（前3行）:")
        print("-" * 60)

        for i in range(min(3, len(df))):
            text = df[self.target_column].iloc[i]
            if pd.notna(text):
                preview = str(text)[:100] + "..." if len(str(text)) > 100 else str(text)
                print(f"行 {i + 1}: {preview}")
        print("-" * 60)


def main():
    """测试函数"""
    replacer = CircleNumberReplacer()
    replacer.run(fix_encoding=True)  # fix_encoding=True 会先修复编码乱码


if __name__ == "__main__":
    main()