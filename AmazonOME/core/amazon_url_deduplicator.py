from __future__ import annotations

import re
import pandas as pd
from pathlib import Path


class AmazonURLDeduplicator:
    """Amazon URL查重工具 - 基于ASIN去重"""

    PATTERN = re.compile(r'/(?:dp|pd)/([A-Z0-9]{10})/?')

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.df: pd.DataFrame | None = None

    def load(self) -> "AmazonURLDeduplicator":
        """加载Excel文件"""
        print(f"加载文件: {self.file_path}")
        self.df = pd.read_excel(self.file_path)
        print(f"共 {len(self.df)} 行数据")
        return self

    def extract_asin(self, url: str) -> str | None:
        """从URL中提取ASIN作为查重键"""
        if pd.isna(url):
            return None

        match = self.PATTERN.search(str(url))
        if match:
            return match.group(1)
        return None

    def deduplicate(self) -> "AmazonURLDeduplicator":
        """执行ASIN查重删除"""
        if self.df is None:
            raise ValueError("请先调用load()加载数据")

        if len(self.df.columns) == 0:
            raise ValueError("Excel文件没有列")

        df: pd.DataFrame = self.df
        url_column = df.columns[0]
        print(f"处理列: {url_column}")

        asin_series = df[url_column].apply(self.extract_asin)

        total = len(df)
        with_asin = asin_series.notna().sum()
        no_asin = total - with_asin

        print(f"提取到ASIN: {with_asin} 行")
        print(f"无ASIN: {no_asin} 行（将删除）")

        mask = asin_series.notna()
        df_valid = df[mask].copy()
        df_valid['__asin__'] = asin_series[mask]

        before_dedup = len(df_valid)
        df_deduped = df_valid.drop_duplicates(subset=['__asin__'], keep='first')
        after_dedup = len(df_deduped)
        removed = before_dedup - after_dedup

        print(f"查重前: {before_dedup} 行")
        print(f"删除重复: {removed} 行")
        print(f"查重后: {after_dedup} 行")

        df_deduped = df_deduped.drop(columns=['__asin__'])
        self.df = df_deduped

        print(f"最终保留: {len(self.df)} 行")

        return self

    def save(self) -> "AmazonURLDeduplicator":
        """保存到原文件（覆盖）"""
        if self.df is None:
            raise ValueError("没有数据可保存")

        self.df.to_excel(self.file_path, index=False)
        print(f"已保存到原文件: {self.file_path}")

        return self

    def run_instance(self) -> None:
        """实例方法：执行完整流程"""
        self.load().deduplicate().save()

    @classmethod
    def run(cls, file_path: str | Path | None = None) -> None:
        """
        类方法：直接通过类调用，自动处理路径查找和实例化

        使用方式: AmazonURLDeduplicator.run()
        """
        if file_path is None:
            input_file = Path("data/output/result.xlsx")

            if not input_file.exists():
                alternatives = [
                    "result.xlsx",
                    "output/result.xlsx",
                    "../data/output/result.xlsx",
                    "data/result.xlsx",
                ]
                for alt in alternatives:
                    if Path(alt).exists():
                        input_file = Path(alt)
                        break
                else:
                    raise FileNotFoundError("找不到默认文件，请指定路径")
        else:
            input_file = Path(file_path)

        # 创建实例并执行
        instance = cls(input_file)
        instance.run_instance()


def main():
    """命令行入口"""
    AmazonURLDeduplicator.run()


if __name__ == "__main__":
    main()