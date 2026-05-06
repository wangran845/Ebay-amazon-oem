from __future__ import annotations

import re
import json
import random
import requests
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from data.config import Config


class SecondaryProcessor:
    """二次处理：动态品牌库 + 三列清洗 + 统一货币为美元"""

    # 汇率缓存（类变量）
    _exchange_rates = {}
    _rate_update_time = None

    # 备用兜底汇率
    FALLBACK_RATES = {
        'USD': 1.0,
        'CNY': 0.138,
        'EUR': 1.08,
        'GBP': 1.27,
        'JPY': 0.0067,
        'AUD': 0.65,
        'CAD': 0.73,
        'CHF': 1.02,
        'HKD': 0.128
    }

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径配置
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_初步删选.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_二次处理.xlsx")

        # 动态品牌文件路径（会在run中确定）
        self.brand_file = None  # Brand_{key_W}.xlsx
        self.base_brand_file = Path("data/Brand.xlsx")  # 原始品牌库

        # 参数配置
        self.api_timeout = 5
        self.brands = []
        self.key_w = ""  # 从category提取的key

    # ==================== 工具方法 ====================

    @staticmethod
    def _clean_invisible_chars(text: str) -> str:
        """清理零宽字符和控制字符"""
        if not isinstance(text, str):
            return text
        return re.sub(r'[\u200b-\u200f\ufeff]', '', text)

    @staticmethod
    def _clean_spaces(text: str) -> str:
        """清理中英文空格"""
        if not isinstance(text, str):
            return text
        # 替换所有空格（包括中文全角空格\u3000、英文空格等）
        return re.sub(r'[\s\u3000]+', '', text)

    def _extract_key_w(self, category: str) -> str:
        """
        从category提取key_W：最后一个›后内容，去掉中英文空格
        例如：'Car & Truck Parts › Engines & Components › Fuel Injectors'
        → 'FuelInjectors'
        """
        if pd.isna(category) or not str(category).strip():
            return "unknown"

        text = str(category).strip()
        # 查找最后一个 › 或 > 后的内容
        for sep in [' › ', ' > ', '›', '>']:
            if sep in text:
                text = text.split(sep)[-1]

        # 去掉中英文空格
        return self._clean_spaces(text)

    # ==================== 动态品牌库管理 ====================

    def setup_brand_file(self, df: pd.DataFrame) -> None:
        """
        确定key_W并设置品牌文件路径
        从category列最后一行（或第一行非空值）提取
        """
        # 查找category列
        category_col = None
        for name in ['category', 'Category', 'CATEGORY', 'categories']:
            if name in df.columns:
                category_col = name
                break

        if category_col is None:
            print("⚠️  警告: 未找到category列，使用默认key 'unknown'")
            self.key_w = "unknown"
        else:
            # 获取最后一个非空的category值
            categories = df[category_col].dropna()
            if len(categories) == 0:
                print("⚠️  警告: category列全为空，使用默认key 'unknown'")
                self.key_w = "unknown"
            else:
                last_category = categories.iloc[-1]  # 最后一行
                self.key_w = self._extract_key_w(last_category)
                print(f"      提取到category: {last_category}")
                print(f"      生成key_W: {self.key_w}")

        # 设置动态品牌文件路径
        self.brand_file = Path(f"data/Brand_{self.key_w}.xlsx")
        print(f"      品牌文件: {self.brand_file}")

    def collect_brand_other(self, df: pd.DataFrame) -> set:
        """
        从Brand_Other列收集所有非白名单品牌
        """
        if 'Brand_Other' not in df.columns:
            print("⚠️  警告: 未找到Brand_Other列，无法收集新品牌")
            return set()

        brands = set()
        for value in df['Brand_Other'].dropna():
            clean = self._clean_invisible_chars(str(value)).strip()
            if clean:
                brands.add(clean)

        print(f"      从Brand_Other收集到 {len(brands)} 个新品牌")
        return brands

    def update_brand_file(self, new_brands: set) -> None:
        """
        更新Brand_{key_W}.xlsx：
        - 如果存在，追加新品牌（去重）
        - 如果不存在，创建新文件
        """
        existing_brands = set()

        # 先加载基础品牌库（如果存在）
        if self.base_brand_file.exists():
            try:
                df_base = pd.read_excel(self.base_brand_file)
                if 'Brand' in df_base.columns:
                    existing_brands.update(
                        self._clean_invisible_chars(str(b)).strip()
                        for b in df_base['Brand'].dropna()
                    )
                    print(f"      加载基础品牌库: {len(existing_brands)} 个")
            except Exception as e:
                print(f"      加载基础品牌库失败: {e}")

        # 如果动态品牌文件已存在，加载它
        if self.brand_file.exists():
            try:
                df_existing = pd.read_excel(self.brand_file)
                if 'Brand' in df_existing.columns:
                    existing_brands.update(
                        self._clean_invisible_chars(str(b)).strip()
                        for b in df_existing['Brand'].dropna()
                    )
                    print(f"      加载现有动态品牌库: {len(existing_brands)} 个")
            except Exception as e:
                print(f"      加载现有动态品牌库失败: {e}")

        # 合并新旧品牌
        all_brands = existing_brands.union(new_brands)

        # 保存
        self.brand_file.parent.mkdir(parents=True, exist_ok=True)
        df_brands = pd.DataFrame({'Brand': sorted(all_brands)})
        df_brands.to_excel(self.brand_file, index=False)
        print(f"      品牌库已更新: {len(all_brands)} 个品牌 → {self.brand_file}")

    def load_brands(self):
        """加载品牌数据库（优先使用Brand_{key_W}.xlsx）"""
        print(f"[1/5] 读取品牌数据库...")

        # 优先使用动态品牌文件
        target_file = self.brand_file if self.brand_file and self.brand_file.exists() else self.base_brand_file

        if not target_file or not target_file.exists():
            print(f"⚠️  警告: 品牌文件不存在，跳过品牌清洗")
            self.brands = []
            return

        try:
            df_brands = pd.read_excel(target_file)

            if 'Brand' not in df_brands.columns:
                print(f"⚠️  警告: 品牌文件中未找到 'Brand' 列")
                self.brands = []
                return

            # 获取品牌列表，清理、去重、按长度降序排序
            self.brands = df_brands['Brand'].dropna().astype(str).unique().tolist()
            self.brands = [
                self._clean_invisible_chars(b.strip())
                for b in self.brands
                if b.strip()
            ]
            self.brands.sort(key=len, reverse=True)

            print(f"      成功加载 {len(self.brands)} 个品牌（来自 {target_file.name}）")
        except Exception as e:
            print(f"⚠️  加载品牌文件失败: {e}")
            self.brands = []

    # ==================== 品牌清洗（新逻辑） ====================

    def clean_title(self, title: str) -> str:
        """
        清洗Title列：直接删除整个品牌词（整词匹配）
        """
        if pd.isna(title) or not self.brands:
            return title

        title = self._clean_invisible_chars(str(title))

        for brand in self.brands:
            if not brand:
                continue

            # 整词匹配（支持's结尾）
            if brand.isascii():
                # 匹配 brand 或 brand's/Brand's（不区分大小写）
                pattern = r'\b' + re.escape(brand) + r"(?:'s)?\b"
            else:
                pattern = r'(?<![a-zA-Z0-9])' + re.escape(brand) + r"(?:'s)?(?![a-zA-Z0-9])"

            title = re.sub(pattern, '', title, flags=re.IGNORECASE)

        # 清理多余空格和首尾标点
        title = re.sub(r'\s+', ' ', title).strip()
        title = re.sub(r'^[-|/\s]+|[-|/\s]+$', '', title)

        return title if title.strip() else ""

    def clean_text_replace(self, text: str) -> str:
        """
        清洗description/About列：直接删除品牌
        支持 brand's/Brand's 形式
        """
        if pd.isna(text) or not self.brands:
            return text

        text = self._clean_invisible_chars(str(text))

        for brand in self.brands:
            if not brand:
                continue

            # 整词匹配，支持's所有格，直接删除
            if brand.isascii():
                pattern = r'\b' + re.escape(brand) + r"(?:'s)?\b"
            else:
                pattern = r'(?<![a-zA-Z0-9])' + re.escape(brand) + r"(?:'s)?(?![a-zA-Z0-9])"

            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        # 清理多余空格
        text = re.sub(r'[ \t ]+', ' ', text).strip()

        return text

    # ==================== 价格处理部分（保持不变） ====================

    def get_exchange_rates(self) -> dict:
        """获取实时汇率，失败用备用汇率"""
        # 1小时内复用
        if self._exchange_rates and self._rate_update_time:
            time_diff = (datetime.now() - self._rate_update_time).total_seconds()
            if time_diff < 3600:
                return self._exchange_rates

        self._exchange_rates = self.FALLBACK_RATES.copy()

        try:
            resp = requests.get(
                "https://open.er-api.com/v6/latest/USD",
                timeout=self.api_timeout,
                allow_redirects=False
            )
            if resp.status_code == 200 and resp.json().get('result') == 'success':
                self._exchange_rates = resp.json()['rates']
                self._exchange_rates['USD'] = 1.0
        except Exception as e:
            print(f"      实时汇率API失败，使用备用汇率: {e}")

        self._rate_update_time = datetime.now()
        return self._exchange_rates

    def process_price(self, text: str) -> str:
        """
        价格处理：识别币种 → 提取金额 → 换算为美元 → 输出纯数字(保留两位小数)
        """
        if pd.isna(text) or str(text).strip() == '':
            return text

        t = str(text).strip()
        t_lower = t.lower()
        currency = 'USD'

        # 识别币种
        prefix_pattern = r'^(AU|AUD|GBP|USD|EUR|CNY|JPY|CAD|CHF|HKD)\s*[\$\£\€\¥]?'
        prefix_match = re.match(prefix_pattern, t, re.IGNORECASE)
        if prefix_match:
            currency = prefix_match.group(1).upper()
            if currency == 'AU':
                currency = 'AUD'
        elif '£' in t or 'gbp' in t_lower:
            currency = 'GBP'
        elif '€' in t or 'eur' in t_lower:
            currency = 'EUR'
        elif '¥' in t and 'jp' not in t_lower:
            currency = 'CNY'
        elif 'jp¥' in t or 'jpy' in t_lower:
            currency = 'JPY'
        elif 'aud' in t_lower:
            currency = 'AUD'

        # 提取金额
        num_pattern = r'(\d+(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?(?:,\d{3})*)'
        num_matches = re.findall(num_pattern, t)

        if not num_matches:
            return text

        num_str = max(num_matches, key=len).replace(',', '')
        try:
            amount = float(num_str)
        except:
            return text

        # 换算为美元
        rates = self.get_exchange_rates()
        usd_amount = amount * rates.get(currency, 1.0)

        return f"{usd_amount:.2f}"

    # ==================== 主流程 ====================

    def run(self) -> None:
        """主处理流程"""
        print(f"===== 二次处理：动态品牌库 + 三列清洗 + 货币统一 =====")
        print(f"[0/5] 读取源文件: {self.src_file}")

        df = pd.read_excel(self.src_file)
        print(f"      成功读取 {len(df)} 行数据")
        print(f"      可用列: {list(df.columns)}")

        # 0. 确定key_W并设置品牌文件
        self.setup_brand_file(df)

        # 1. 收集Brand_Other并更新品牌库
        print(f"[1/5] 更新动态品牌库...")
        new_brands = self.collect_brand_other(df)
        if new_brands:
            self.update_brand_file(new_brands)
        else:
            print("      无新品牌需要添加")

        # 2. 加载品牌（使用更新后的动态品牌库）
        self.load_brands()

        # 3. 清洗 Title 列（直接删除品牌）
        if 'Title' not in df.columns:
            print(f"⚠️  警告: 未找到 'Title' 列")
        else:
            print(f"[2/5] 清洗 Title 列（直接删除品牌名）...")
            df['Title'] = df['Title'].apply(self.clean_title)
            print("      Title清洗完成")

        # 4. 清洗 description_from_the_seller 列（替换为OKcarpart）
        if 'description_from_the_seller' not in df.columns:
            print(f"⚠️  警告: 未找到 'description_from_the_seller' 列")
        else:
            print(f"[3/5] 清洗 description_from_the_seller 列（替换为OKcarpart）...")
            df['description_from_the_seller'] = df['description_from_the_seller'].apply(self.clean_text_replace)
            print("      description清洗完成")

        # 5. 清洗 About_table 列（替换为OKcarpart）
        about_col = None
        for name in ['About_table', 'About', 'about_table', 'about']:
            if name in df.columns:
                about_col = name
                break

        if about_col is None:
            print(f"⚠️  警告: 未找到 About_table 列")
        else:
            print(f"[4/5] 清洗 '{about_col}' 列（替换为OKcarpart）...")
            df[about_col] = df[about_col].apply(self.clean_text_replace)
            print("      About_table清洗完成")

        # 6. 处理 price 列
        if 'price' not in df.columns:
            print(f"⚠️  警告: 未找到 'price' 列")
        else:
            print(f"[5/5] 处理 price 列（统一为美元）...")
            df['price'] = df['price'].apply(self.process_price)
            print("      价格处理完成")

        # 7. 保存
        print(f"[保存] 保存到: {self.dst_file}")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)
        print(f"\n✅ 二次处理完成！")
        print(f"   使用品牌库: Brand_{self.key_w}.xlsx")


def main():
    """供 main(1).py 调用的入口"""
    processor = SecondaryProcessor()
    processor.run()


if __name__ == "__main__":
    main()