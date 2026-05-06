from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path

from data.config import Config


class ProductUrlGenerator:
    """
    生成产品URL和相关信息表格
    读取.csv和_已格式化.xlsx，生成包含OEM、URL、SKU、名称等列的表格
    """

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 源文件路径
        self.formatted_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_已格式化.xlsx")
        self.csv_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_已格式化.csv")

        # 目标文件路径
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_tdk.xlsx")

        # 基础URL
        self.base_url = "https://okcarpart.com/"

        # 目标列名
        self.target_columns = [
            "OEM",
            "空白列",
            "url_key",
            "url_new",
            "sku",
            "html_file",
            "name",
            "meta_keyword",
            "meta_description"
        ]

    def read_formatted_excel(self) -> pd.DataFrame:
        """读取已格式化的Excel文件，获取OEM列"""
        if not self.formatted_file.exists():
            raise FileNotFoundError(f"文件不存在: {self.formatted_file}")

        df = pd.read_excel(self.formatted_file)
        print(f"✓ 读取已格式化文件: {self.formatted_file}")
        print(f"  行数: {len(df)}")
        print(f"  列名: {list(df.columns)}")

        # 检查OEM列是否存在
        if "OEM" not in df.columns:
            # 尝试其他可能的列名
            possible_names = ["oem", "Oem", "OE", "Oe", "oe"]
            for name in possible_names:
                if name in df.columns:
                    df = df.rename(columns={name: "OEM"})
                    break
            else:
                raise ValueError(f"未找到OEM列，可用列: {list(df.columns)}")

        return df

    def read_csv_file(self) -> pd.DataFrame:
        """读取CSV文件，获取url_key、sku、name列"""
        if not self.csv_file.exists():
            raise FileNotFoundError(f"文件不存在: {self.csv_file}")

        df = pd.read_csv(self.csv_file, encoding='utf-8')
        print(f"✓ 读取CSV文件: {self.csv_file}")
        print(f"  行数: {len(df)}")
        print(f"  列名: {list(df.columns)}")

        # 检查所需列是否存在
        required_cols = ["url_key", "sku", "name"]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            # 尝试其他可能的列名
            col_mapping = {
                "url_key": ["url_key", "urlkey", "url key", "URL_KEY"],
                "sku": ["sku", "SKU", "part_number", "partnumber"],
                "name": ["name", "Name", "NAME", "product_name", "productname"]
            }

            for std_name, possible_names in col_mapping.items():
                if std_name in missing_cols:
                    for p_name in possible_names:
                        if p_name in df.columns:
                            df = df.rename(columns={p_name: std_name})
                            missing_cols.remove(std_name)
                            break

            if missing_cols:
                raise ValueError(f"未找到列: {missing_cols}，可用列: {list(df.columns)}")

        return df

    def merge_data(self, formatted_df: pd.DataFrame, csv_df: pd.DataFrame) -> pd.DataFrame:
        """合并两个数据源"""
        # 检查行数是否匹配
        if len(formatted_df) != len(csv_df):
            print(f"⚠️  警告: 行数不匹配 - 格式化文件: {len(formatted_df)}, CSV文件: {len(csv_df)}")
            print(f"   将使用较小的行数进行合并")

        # 获取最小行数
        min_rows = min(len(formatted_df), len(csv_df))

        # 创建结果DataFrame
        result_df = pd.DataFrame()

        # 添加OEM列（从格式化文件读取）
        result_df["OEM"] = formatted_df["OEM"].iloc[:min_rows].reset_index(drop=True)

        # 添加空白列（统一为base_url）
        result_df["空白列"] = self.base_url

        # 从CSV读取数据
        result_df["url_key"] = csv_df["url_key"].iloc[:min_rows].reset_index(drop=True)
        result_df["sku"] = csv_df["sku"].iloc[:min_rows].reset_index(drop=True)
        result_df["name"] = csv_df["name"].iloc[:min_rows].reset_index(drop=True)

        # 生成url_new（空白列 + url_key）
        result_df["url_new"] = result_df["空白列"] + result_df["url_key"].astype(str)

        # 生成html_file（sku + .html）
        # 先转换为字符串，去除可能的空值，然后添加.html后缀
        sku_series = result_df["sku"].astype(str)
        # 处理空值或NaN的情况
        sku_series = sku_series.replace('nan', '').replace('None', '')
        result_df["html_file"] = sku_series + '.html'

        # 如果sku为空，html_file只保留.html，这里可以选择保留为空或设置为.html
        # 如果想保持空值，可以使用下面的逻辑：
        # result_df["html_file"] = sku_series.apply(lambda x: x + '.html' if x and x not in ['nan', 'None', ''] else '')

        # 添加空列
        result_df["meta_keyword"] = ""
        result_df["meta_description"] = ""

        # 按指定顺序排列列
        result_df = result_df[self.target_columns]

        return result_df

    def run(self) -> None:
        """
        主流程
        """
        print("=" * 60)
        print("[ProductUrlGenerator] 开始生成URL表格")
        print("=" * 60)

        try:
            # 读取源文件
            print("\n[1/4] 读取源文件...")
            formatted_df = self.read_formatted_excel()
            csv_df = self.read_csv_file()

            # 合并数据
            print("\n[2/4] 合并数据...")
            result_df = self.merge_data(formatted_df, csv_df)

            # 显示统计信息
            print(f"\n[3/4] 生成结果统计:")
            print(f"  生成行数: {len(result_df)}")
            print(f"  列数: {len(result_df.columns)}")
            print(f"  列: {list(result_df.columns)}")

            # 显示URL样例
            unique_urls = result_df["url_new"].nunique()
            print(f"  唯一URL数: {unique_urls}")

            # 显示html_file样例
            non_empty_html = (result_df["html_file"] != '.html').sum()
            print(f"  非空html_file数: {non_empty_html}")

            # 保存文件
            print("\n[4/4] 保存文件...")
            self.dst_file.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_excel(self.dst_file, index=False)

            print(f"\n✅ 成功保存到: {self.dst_file.resolve()}")
            print(f"   文件大小: {self.dst_file.stat().st_size / 1024:.2f} KB")

            # 显示样例
            self._show_sample(result_df)

        except Exception as e:
            print(f"\n❌ 错误: {e}")
            raise

    def _show_sample(self, df: pd.DataFrame) -> None:
        """显示处理后的样例数据"""
        print("\n" + "=" * 60)
        print("📊 数据样例（前5行）:")
        print("=" * 60)

        # 设置显示选项
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)

        print(df.head(5).to_string())
        print("=" * 60)

        # 显示null值统计
        print("\n📊 数据完整性检查:")
        null_counts = df.isnull().sum()
        for col in df.columns:
            if null_counts[col] > 0:
                print(f"  ⚠️  {col}: {null_counts[col]} 个空值")
            else:
                print(f"  ✓ {col}: 完整")

        # 显示URL生成样例
        print("\n🔗 URL生成样例:")
        for i in range(min(3, len(df))):
            print(f"  行{i + 1}: {df['url_key'].iloc[i]} → {df['url_new'].iloc[i]}")

        # 显示html_file样例
        print("\n📄 HTML文件样例:")
        for i in range(min(3, len(df))):
            sku_value = df['sku'].iloc[i] if pd.notna(df['sku'].iloc[i]) else "空"
            print(f"  行{i + 1}: SKU={sku_value} → {df['html_file'].iloc[i]}")


def main():
    """测试函数"""
    generator = ProductUrlGenerator()
    generator.run()


if __name__ == "__main__":
    main()