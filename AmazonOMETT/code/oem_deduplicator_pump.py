from __future__ import annotations

import pandas as pd
from pathlib import Path
from data.config import Config
from collections import Counter
import re


class OEMDeduplicator:
    """OEM去重：优先保留评论最多的，无评论则按Price+About_table长度排序"""

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_name = self.config.product_user_2

        # 路径
        self.src_file = Path(f"data/{self.product_name}/{self.product_name}_最终.xlsx")
        self.dst_file = Path(f"data/{self.product_name}/{self.product_name}_OEM去重后.xlsx")

        # 列名缓存（在读取数据后填充）
        self.ome_col = None
        self.price_col = None
        self.item_col = None
        self.about_col = None
        self.desc_col = None
        self.category_col = None
        self.review_col = None  # seller_feedback评论列

    # ==================== 步骤1: 数据读取 ====================
    def load_data(self) -> pd.DataFrame | None:
        """[1/7] 读取源文件"""
        print(f"[1/7] 读取源文件: {self.src_file}")

        if not self.src_file.exists():
            print(f"❌ 错误: 文件不存在 - {self.src_file}")
            return None

        df = pd.read_excel(self.src_file)
        print(f"      成功读取 {len(df)} 行数据")
        print(f"      可用列: {list(df.columns)}")
        return df

    # ==================== 步骤2: 列名识别 ====================
    def detect_columns(self, df: pd.DataFrame) -> bool:
        """[2/7] 自动识别各列名，返回是否成功"""
        print(f"[2/7] 识别列名...")

        # OME列
        for col in ['OEM', 'OME', 'oem', 'ome']:
            if col in df.columns:
                self.ome_col = col
                break

        if not self.ome_col:
            print(f"❌ 错误: 未找到OME列")
            return False
        print(f"      ✓ OEM列: {self.ome_col}")

        # Price列
        for col in ['price', 'Price', '价格']:
            if col in df.columns:
                self.price_col = col
                break
        if self.price_col:
            print(f"      ✓ Price列: {self.price_col}")
        else:
            print(f"      ⚠️ 未找到price列")

        # Item_specifics列
        for col in ['item_specifics', 'Item Specifics', 'item specifics', '核心参数']:
            if col in df.columns:
                self.item_col = col
                break
        if self.item_col:
            print(f"      ✓ Item_specifics列: {self.item_col}")
        else:
            print(f"      ⚠️ 未找到item_specifics列")

        # About_table列
        for col in ['About_table', 'about_table', 'About', 'about']:
            if col in df.columns:
                self.about_col = col
                break
        if not self.about_col:
            print(f"❌ 错误: 未找到About_table列")
            return False
        print(f"      ✓ About_table列: {self.about_col}")

        # Description列
        for col in ['description_from_the_seller', 'Description', 'description']:
            if col in df.columns:
                self.desc_col = col
                break
        if self.desc_col:
            print(f"      ✓ Description列: {self.desc_col}")
        else:
            print(f"      ⚠️ 未找到description_from_the_seller列")

        # Category列（仅统计用）
        for col in ['category', 'Category', '产品类目', '类目']:
            if col in df.columns:
                self.category_col = col
                break
        if self.category_col:
            print(f"      ✓ Category列: {self.category_col}")

        # Review列（seller_feedback）
        for col in ['seller_feedback', '评论', 'review', 'Review', 'reviews']:
            if col in df.columns:
                self.review_col = col
                break
        if self.review_col:
            print(f"      ✓ Review列: {self.review_col}")
        else:
            print(f"      ⚠️ 未找到seller_feedback列，将使用原有逻辑")

        return True

    # ==================== 步骤3: 数据筛选 ====================
    def filter_valid_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """[3/7] 筛选有效price（不为空且不为0）"""
        if not self.price_col:
            print(f"[3/7] 跳过price筛选（无price列）")
            return df

        print(f"[3/7] 筛选有效price...")
        valid_mask = df[self.price_col].apply(self._is_valid_price)
        deleted = (~valid_mask).sum()
        df = df[valid_mask].reset_index(drop=True)
        print(f"      删除 {deleted} 行无效price，剩余 {len(df)} 行")
        return df

    def _is_valid_price(self, price) -> bool:
        """检查price是否有效（不为空且不为0且不低于80）"""
        if pd.isna(price):
            return False
        price_str = str(price).strip()
        if not price_str:
            return False
        try:
            return float(price_str) >= 80
        except:
            return False

    # ==================== 步骤3.5: 标题年份筛选 ====================
    def _is_valid_year(self, Title) -> bool:
        """检查产品年份是否有效（2010年及之后），清洗2010年前的产品"""
        if pd.isna(Title):
            return False
        title_str = str(Title).strip()
        if not title_str:
            return False

        import re

        # 匹配多种年份格式（支持19xx和20xx）
        year_patterns = [
            r'(?:19|20)\d{2}\s*-\s*(?:19|20)\d{2}',  # 1980-2019, 2016-2019（非捕获组）
            r'(?:19|20)\d{2}\s+(?:19|20)\d{2}',  # 1980 2013（空格分隔）
            r'(?:19|20)\d{2}/\d{2}-(?:19|20)\d{2}/\d{2}',  # 2008/06-2015/12
            r'(?:19|20)\d{2}/\d{2}\s*-\s*(?:19|20)\d{2}/\d{2}',  # 带空格版本
            r'(?:19|20)\d{2}',  # 单独年份1980, 2016等（非捕获组）
        ]

        years_found = []
        for pattern in year_patterns:
            matches = re.findall(pattern, title_str)
            for match in matches:
                # 提取所有4位年份数字（19xx或20xx）
                years = re.findall(r'(?:19|20)\d{2}', match)  # 这里也改非捕获组
                years_found.extend([int(y) for y in years])

        # 去重并排序
        years_found = sorted(set(years_found))

        # 没有找到任何年份，视为无效
        if not years_found:
            return True

        # 最小年份≥2010才有效（1980会被筛除）
        min_year = min(years_found)
        return min_year >= 2010

    def filter_valid_item_specifics(self, df: pd.DataFrame) -> pd.DataFrame:
        """[4/7] 筛选有效item_specifics（有内容）"""
        if not self.item_col:
            print(f"[4/7] 跳过item_specifics筛选（无该列）")
            return df

        print(f"[4/7] 筛选有效 {self.item_col}...")
        valid_mask = df[self.item_col].apply(self._has_item_specifics)
        deleted = (~valid_mask).sum()
        df = df[valid_mask].reset_index(drop=True)
        print(f"      删除 {deleted} 行空item_specifics，剩余 {len(df)} 行")
        return df

    def _has_item_specifics(self, text) -> bool:
        """检查item_specifics是否有值"""
        if pd.isna(text):
            return False
        text_str = str(text).strip()
        if not text_str:
            return False
        # 如果已经是HTML，检查是否有实际内容
        if text_str.startswith("<"):
            clean_text = re.sub(r'<[^>]+>', '', text_str).strip()
            return len(clean_text) > 0
        return True

    # ==================== 步骤4: 统计信息 ====================
    def show_category_stats(self, df: pd.DataFrame) -> None:
        """[5/7] 统计产品类目频次（仅信息展示）"""
        if not self.category_col:
            print(f"[5/7] 跳过类目统计（无category列）")
            return

        print(f"[5/7] 统计产品类目频次...")
        category_counts = Counter(df[self.category_col].dropna().astype(str))
        print(f"      共 {len(category_counts)} 个类目，Top3: {list(category_counts.keys())[:3]}")

    # ==================== 步骤5: 计算排序权重 ====================
    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """[6/7] 计算排序权重：优先评论数，无评论则Price 60% + About_table长度 40%"""
        print(f"[6/7] 计算排序权重...")

        # 计算评论数（如果存在review列）
        if self.review_col:
            df['_review_count'] = df[self.review_col].apply(self._count_reviews)
            has_reviews = (df['_review_count'] > 0).sum()
            print(f"      ✓ 评论列存在，{has_reviews} 行有评论")
        else:
            df['_review_count'] = 0
            print(f"      ⚠️ 无评论列，全部按原有逻辑排序")

        # 计算About_table字节长度并归一化
        df['_byte_length'] = df[self.about_col].apply(
            lambda x: len(str(x).encode('utf-8')) if pd.notna(x) else 0
        )
        max_byte = df['_byte_length'].max()
        df['_byte_norm'] = df['_byte_length'] / max_byte if max_byte > 0 else 0

        # 计算Price并归一化（价格越高值越大）
        if self.price_col:
            df['_price_num'] = df[self.price_col].apply(
                lambda x: float(x) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else 0
            )
            max_price = df['_price_num'].max()
            df['_price_norm'] = df['_price_num'] / max_price if max_price > 0 else 0
        else:
            df['_price_norm'] = 0

        # 原有逻辑的综合权重：Price 60% + About_table长度 40%
        df['_base_score'] = df['_price_norm'] * 0.6 + df['_byte_norm'] * 0.4

        # 最终排序策略：
        # 1. 有评论的优先（review_count > 0的排在最前面）
        # 2. 评论数相同的，按原有逻辑（Price+About_table）排序
        # 实现方式：用两列排序，先按_review_count降序，再按_base_score降序

        print(f"      排序策略: 评论数降序 → Price60%+About40%降序")
        return df

    def _count_reviews(self, text) -> int:
        """计算单元格中的评论数量（按分隔符估算）"""
        if pd.isna(text):
            return 0
        text_str = str(text).strip()
        if not text_str:
            return 0
        # 常见评论分隔符：换行符、|、•、★ 等
        # 如果包含这些分隔符，按分隔符数量+1估算
        separators = ['\n','', '|', '•', '★', '☆', '⭐', 'out of 5 stars']
        count = 0
        for sep in separators:
            if sep in text_str:
                count += text_str.count(sep)
        # 如果检测到分隔符，认为至少有count个评论片段
        # 否则如果文本长度>20，认为有1条评论
        if count > 0:
            return max(1, count // 2)  # 保守估计
        return 1 if len(text_str) > 20 else 0

    # ==================== 步骤6: 排序与去重 ====================
    def sort_and_deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """[7/7] 按权重排序并按OEM去重"""
        print(f"[7/7] 按权重排序并去重...")

        # 排序：先按评论数降序，再按基础分数降序
        sort_columns = ['_review_count', '_base_score']
        ascending_flags = [False, False]  # 都是降序

        df_sorted = df.sort_values(by=sort_columns, ascending=ascending_flags)

        # 按OME分组，保留第一行（评论最多或分数最高的）
        df_dedup = df_sorted.drop_duplicates(subset=[self.ome_col], keep='first')

        dedup_count = len(df) - len(df_dedup)
        print(f"      去重完成: 删除 {dedup_count} 行，剩余 {len(df_dedup)} 行")

        # 显示去重示例
        print(f"去重示例（部分）:")
        dup_omes = df[self.ome_col].value_counts().head(3)
        for ome, count in dup_omes.items():
            if count > 1:
                # 显示保留的是哪一行（评论数或分数）
                kept_row = df_dedup[df_dedup[self.ome_col] == ome]
                if not kept_row.empty:
                    review_cnt = kept_row.iloc[0].get('_review_count', 0)
                    base_score = kept_row.iloc[0].get('_base_score', 0)
                    if review_cnt > 0:
                        print(f"      {ome}: {count}条 → 保留1条（评论数: {review_cnt}）")
                    else:
                        print(f"      {ome}: {count}条 → 保留1条（分数: {base_score:.3f}）")

        return df_dedup

    # ==================== 步骤8: 格式化输出 ====================
    def format_description_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理description_from_the_seller列格式"""
        if not self.desc_col:
            print(f"[8/8] 跳过description格式化（无该列）")
            return df

        print(f"[8/8] 处理 {self.desc_col} 列格式...")

        # 检查是否已经是HTML
        sample_val = str(df[self.desc_col].iloc[0]) if len(df) > 0 else ""
        if '<table' in sample_val.lower() or '<div' in sample_val.lower():
            print("      检测到已经是HTML格式，跳过<p>标签包裹")
        else:
            df[self.desc_col] = df[self.desc_col].apply(self._format_description)
            print("      已添加<p>和<div>标签")

        return df

    def _format_description(self, text: str) -> str:
        """处理description_from_the_seller为HTML格式"""
        if pd.isna(text) or not str(text).strip():
            return '<div id="description_from_the_seller"></div>'

        text = str(text).strip()

        # 如果已经是HTML，直接返回
        if '<table' in text.lower() or '<div' in text.lower():
            return text

        # 按换行符分割，每行加<p>
        lines = text.split("\n")
        p_lines = [f"<p>{line.strip()}</p>" for line in lines if line.strip()]

        inner_html = "".join(p_lines)
        return f'<div id="description_from_the_seller">{inner_html}</div>'

    # ==================== 步骤9: 保存结果 ====================
    def save_result(self, df: pd.DataFrame, original_count: int, after_price: int) -> None:
        """保存最终结果到Excel"""
        # 清理临时计算列
        temp_cols = ['_review_count', '_byte_length', '_byte_norm',
                     '_price_num', '_price_norm', '_base_score']
        for col in temp_cols:
            if col in df.columns:
                df = df.drop(columns=[col])

        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)

        print(f"✅ 处理完成，已保存到: {self.dst_file}")
        print(f"   总计: 读取 {original_count} 行 → 有效price {after_price} 行 → 去重后 {len(df)} 行")

    # ==================== 主控流程 ====================
    def run(self) -> None:
        """主控调用：按顺序执行各步骤"""
        print(f"===== OEM去重与格式处理 =====")

        # 步骤1: 读取数据
        df = self.load_data()
        if df is None:
            return
        original_count = len(df)

        # 步骤2: 识别列名
        if not self.detect_columns(df):
            return

        # 步骤3: 筛选有效price
        df = self.filter_valid_price(df)
        after_price = len(df)

        # 步骤3.5: 筛选有效年份（2010年及之后）
        # 查找title列
        title_col = None
        for col in ['title', 'Title', 'TITLE', '产品标题', '标题']:
            if col in df.columns:
                title_col = col
                break

        if title_col:
            print(f"[3.5/7] 筛选有效年份（≥2010）...")
            valid_year_mask = df[title_col].apply(self._is_valid_year)
            year_deleted = (~valid_year_mask).sum()
            df = df[valid_year_mask].reset_index(drop=True)
            print(f"      删除 {year_deleted} 行2010年前产品，剩余 {len(df)} 行")
        else:
            print(f"[3.5/7] 跳过年份筛选（无title列）")

        # 步骤4: 筛选有效item_specifics
        df = self.filter_valid_item_specifics(df)

        # 步骤5: 统计类目信息
        self.show_category_stats(df)

        # 步骤6: 计算排序权重（新逻辑：评论优先）
        df = self.calculate_scores(df)

        # 步骤7: 排序并去重
        df_dedup = self.sort_and_deduplicate(df)



        # 步骤9: 保存结果
        self.save_result(df_dedup, original_count, after_price)


def main():
    """供 main(1).py 调用的入口"""
    deduplicator = OEMDeduplicator()
    deduplicator.run()


if __name__ == "__main__":
    main()