from __future__ import annotations

import re
import pandas as pd
from pathlib import Path

from data.config import Config


class DuplicateRemover:
    """
    从URL列中提取/dp/XXXXXXXXXX/作为查重键，删除重复值，保留第一个
    没有提取到dp的行也删除（可能是广告）
    同时将表格中的中文符号全部转换为英文符号
    """

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_need_with_images.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_标题去重.xlsx")

        # 正则：匹配 /dp/XXXXXXXXXX/ (10位字母数字混合的ASIN)
        self.dp_pattern = re.compile(r'/dp/[A-Z0-9]{10}/')

        # 中文符号 -> 英文符号 映射表
        self.punctuation_map = str.maketrans({
            '，': ',',  # 中文逗号
            '。': '.',  # 中文句号
            '；': ';',  # 中文分号
            '：': ':',  # 中文冒号
            '？': '?',  # 中文问号
            '！': '!',  # 中文感叹号
            '（': '(',  # 中文左括号
            '）': ')',  # 中文右括号
            '｛': '{',  # 中文左花括号
            '｝': '}',  # 中文右花括号
            '「': '"',  # 中文左引号
            '」': '"',  # 中文右引号
            '『': "'",  # 中文左单引号
            '』': "'",  # 中文右单引号
            '“': '"',  # 中文左双引号
            '”': '"',  # 中文右双引号
            '‘': "'",  # 中文左单引号
            '’': "'",  # 中文右单引号
            '《': '<',  # 中文左书名号
            '》': '>',  # 中文右书名号
            '、': ',',  # 中文顿号
            '·': '.',  # 中文中点
            '～': '~',  # 中文波浪号
            '—': '-',  # 中文破折号 (em dash)
            '–': '-',  # 中文短横线 (en dash)
            '　': ' ',  # 中文全角空格
            '＠': '@',  # 中文at
            '＃': '#',  # 中文井号
            '＄': '$',  # 中文美元
            '％': '%',  # 中文百分号
            '＆': '&',  # 中文and
            '＊': '*',  # 中文星号
            '＋': '+',  # 中文加号
            '－': '-',  # 中文减号 (全角)
            '①': '1.',
            '②': '2.',
            '③': '3.',
            '④': '4.',
            '⑤': '5.',
            '⑥': '6.',
            '⑦': '7.',
            '⑧': '8.',
            '⑨': '9.',
            '／': '/',  # 中文斜杠
            '＜': '<',  # 中文小于
            '＝': '=',  # 中文等于
            '＞': '>',  # 中文大于
            '？': '?',  # 中文问号 (全角)
            '［': '[',  # 中文左方括号 (全角)
            '＼': '\\',  # 中文反斜杠
            '］': ']',  # 中文右方括号 (全角)
            '＾': '^',  # 中文脱字符
            '＿': '_',  # 中文下划线
            '｀': '`',  # 中文反引号
            '｜': '|',  # 中文竖线
        })

    def _extract_dp_key(self, url: str) -> str | None:
        """从URL中提取 /dp/XXXXXXXXXX/ 部分"""
        if pd.isna(url):
            return None
        url_str = str(url)
        match = self.dp_pattern.search(url_str)
        return match.group(0) if match else None

    def _normalize_punctuation(self, text) -> str:
        """将字符串中的中文符号转换为英文符号"""
        if pd.isna(text):
            return text
        # 如果是数字或布尔值，直接返回
        if isinstance(text, (int, float, bool)):
            return text
        return str(text).translate(self.punctuation_map)

    def _clean_dataframe_punctuation(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗整个DataFrame的中文符号"""
        print("[DuplicateRemover] 开始转换中文符号为英文符号...")

        # 记录修改统计
        modified_cells = 0
        total_cells = 0

        df_cleaned = df.copy()

        for col in df_cleaned.columns:
            # 只处理字符串类型的列
            if df_cleaned[col].dtype == object:
                # 检查每一行是否有中文符号需要转换
                for idx in df_cleaned.index:
                    original = df_cleaned.at[idx, col]
                    if pd.notna(original) and isinstance(original, str):
                        total_cells += 1
                        converted = self._normalize_punctuation(original)
                        if converted != original:
                            df_cleaned.at[idx, col] = converted
                            modified_cells += 1

        if modified_cells > 0:
            print(f"                   转换完成: 扫描 {total_cells} 个单元格, 修改 {modified_cells} 处")
        else:
            print("                   未发现需要转换的中文符号")

        return df_cleaned

    def run(self) -> None:
        """主流程"""
        print(f"[DuplicateRemover] 读取: {self.src_file}")
        df = pd.read_excel(self.src_file)

        before_count = len(df)
        print(f"                   成功读取 {before_count} 行数据")

        # 检查URL列是否存在（支持大小写变体）
        url_col = None
        for col in df.columns:
            if col.lower() == 'url':
                url_col = col
                break

        if not url_col:
            print(f"⚠️  警告: 未找到URL列，可用列: {list(df.columns)}")
            return

        print(f"[DuplicateRemover] 查重列: {url_col} (提取 /dp/XXXXXXXXXX/ 模式)")

        # 提取dp键用于查重
        df['_dp_key'] = df[url_col].apply(self._extract_dp_key)

        # 统计无法提取的数量（这些将被删除）
        null_dp_count = df['_dp_key'].isna().sum()
        if null_dp_count > 0:
            print(f"🗑️  将删除 {null_dp_count} 行未找到 /dp/ 的数据（可能是广告）")

        # 只保留有dp键的行
        df_with_dp = df[df['_dp_key'].notna()].copy()

        if len(df_with_dp) == 0:
            print("⚠️  警告: 没有提取到任何 /dp/ 数据，请检查URL格式")
            return

        # 统计重复
        dup_count = df_with_dp['_dp_key'].duplicated().sum()
        print(f"[DuplicateRemover] 重复 /dp/ 数量: {dup_count}")

        # 删除重复，保留第一个
        df_clean = df_with_dp.drop_duplicates(subset=['_dp_key'], keep='first')

        # 删除辅助列
        df_clean = df_clean.drop(columns=['_dp_key'], errors='ignore')

        # ===== 新增：转换中文符号为英文符号 =====
        df_clean = self._clean_dataframe_punctuation(df_clean)
        # ======================================

        after_count = len(df_clean)
        removed = before_count - after_count
        print(f"[DuplicateRemover] 删除后行数: {after_count} (总共删除 {removed} 行)")

        # 保存
        print("[DuplicateRemover] 保存文件...")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_excel(self.dst_file, index=False)
        print(f"\n✅ [DuplicateRemover] 已保存到: {self.dst_file.resolve()}")


def main():
    remover = DuplicateRemover()
    remover.run()


if __name__ == "__main__":
    main()