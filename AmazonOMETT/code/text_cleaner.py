from __future__ import annotations

import pandas as pd
from pathlib import Path

from data.config import Config


class TextCleaner:
    """
    三合一文本清洗：
    1. Unicode数学字符还原为普通字母
    2. 删除 【
    3. 】 替换为 :
    """

    # 数学字母数字符号块范围
    MATH_RANGES = [
        (0x1D400, 0x1D419, 'A'),  # 粗体大写 𝐀-𝐙
        (0x1D41A, 0x1D433, 'a'),  # 粗体小写 𝐚-𝐳
        (0x1D5D4, 0x1D5ED, 'A'),  # 无衬线粗体大写 𝗔-𝗭
        (0x1D5EE, 0x1D607, 'a'),  # 无衬线粗体小写 𝗮-𝘇
        (0x1D608, 0x1D621, 'A'),  # 无衬线斜体大写 𝘈-𝘡
        (0x1D622, 0x1D63B, 'a'),  # 无衬线斜体小写 𝘢-𝘻
        (0x1D63C, 0x1D655, 'A'),  # 无衬线粗斜体大写 𝘼-𝙕
        (0x1D656, 0x1D66F, 'a'),  # 无衬线粗斜体小写 𝙖-𝙯
    ]

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_圈数字替换.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_已格式化.xlsx")

        # 默认处理列（可修改）
        self.target_column = "产品描述"

    def _is_math_char(self, char: str) -> bool:
        """检查是否为数学特殊字符"""
        code = ord(char)
        return any(start <= code <= end for start, end, _ in self.MATH_RANGES)

    def _normalize_char(self, char: str) -> str:
        """单个数学字符还原"""
        code = ord(char)
        for start, end, base in self.MATH_RANGES:
            if start <= code <= end:
                return chr(code - start + ord(base))
        return char

    def clean_text(self, text: str) -> str:
        """清洗单个文本"""
        if pd.isna(text):
            return text

        text = str(text)
        result = []

        for char in text:
            # 规则1: 数学字符还原
            if self._is_math_char(char):
                result.append(self._normalize_char(char))
            # 规则2: 删除 【
            elif char == '【':
                continue
            # 规则3: 】 换成 :
            elif char == '】':
                result.append(':')
            else:
                result.append(char)

        return ''.join(result)

    def run(self) -> None:
        """主流程"""
        print(f"[TextCleaner] 读取: {self.src_file}")
        df = pd.read_excel(self.src_file)
        print(f"              成功读取 {len(df)} 行数据")

        # 查找目标列
        target_col = None
        possible_names = [self.target_column, '产品描述']
        for name in possible_names:
            if name in df.columns:
                target_col = name
                break

        if target_col is None:
            print(f"⚠️  警告: 未找到目标列，可用列: {list(df.columns)}")
            return

        print(f"[TextCleaner] 处理列: {target_col}")

        # 显示示例
        print("转换示例:")
        samples = df[target_col].dropna().head(3)
        for i, val in enumerate(samples, 1):
            cleaned = self.clean_text(val)
            print(f"  {i}. {val!r} -> {cleaned!r}")

        # 执行清洗
        df[target_col] = df[target_col].apply(self.clean_text)

        # 保存
        print("[TextCleaner] 保存文件...")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)
        print(f"\n✅ [TextCleaner] 已保存到: {self.dst_file.resolve()}")


def main():
    cleaner = TextCleaner()
    cleaner.run()


if __name__ == "__main__":
    main()