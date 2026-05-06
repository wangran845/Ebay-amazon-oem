from __future__ import annotations

import re
import json
from pathlib import Path

import django
from django.conf import settings

# 配置 Django（如果没有配置）
if not settings.configured:
    settings.configure(
        EMOJI_IMG_TAG='<img src="{0}" alt="{1}" title="{2}" class="emoji">'
    )
    django.setup()

import emoji
import pandas as pd
from data.config import Config


class HTMLFormatter:
    """HTML格式化处理器：清洗非法字符 + 转HTML表格"""

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_二次处理.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_HTML格式化.xlsx")

    # ==================== 非法字符清洗 ====================

    def clean_text(self, text: str) -> str:
        """
        清洗文本中的非法字符：
        1. 移除emoji/图标（使用emoji库）
        2. 清理所有零宽字符和特殊空格
        3. 统一为标准空格，清理多余空格
        """
        if pd.isna(text):
            return ""

        text = str(text).strip()

        # 1. 移除所有emoji（使用emoji库）
        text = emoji.replace_emoji(text, replace='')

        clean_chars = []

        for char in text:
            char_code = ord(char)

            # 跳过控制字符（保留\t\n\r）
            if char_code < 0x20 and char_code not in (0x09, 0x0A, 0x0D):
                continue

            # 清理零宽字符和特殊空格，替换为标准空格
            if char_code in (
                    0x00A0,  # 不间断空格
                    0x180E,  # 蒙古文元音分隔符
                    0x2000, 0x2001, 0x2002, 0x2003, 0x2004,
                    0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200A,
                    0x200B, 0x200C, 0x200D, 0x200E, 0x200F,
                    0x2028, 0x2029,  # 行/段分隔符
                    0x202F,  # 窄不间断空格
                    0x205F,  # 中等数学空格
                    0x2060,  # 词连接符
                    0x3000,  # 全角空格
                    0xFEFF,  # BOM
                    0xFFFE, 0xFFFF,
            ):
                clean_chars.append(' ')
                continue

            clean_chars.append(char)

        # 合并并清理多余空格
        clean_text = ''.join(clean_chars)
        clean_text = re.sub(r'\s{2,}', ' ', clean_text).strip()

        return clean_text

    # ==================== 新增：通用冒号合并工具方法 ====================

    def _merge_lonely_colons(self, text: str) -> str:
        """
        将单独成行的冒号（: 或 ：）合并到上一行末尾
        用于清洗 About_table, item_specifics, description_from_the_seller
        """
        if not text or not isinstance(text, str):
            return text

        lines = text.split("\n")
        merged_lines = []

        for line in lines:
            stripped = line.strip()
            # 检查是否是单独的冒号（英文: 或中文：）
            if stripped in (':', '：') and merged_lines:
                # 合并到上一行末尾（去除上一行末尾空格后追加）
                merged_lines[-1] = merged_lines[-1].rstrip() + stripped
            else:
                merged_lines.append(line)

        return "\n".join(merged_lines)

    # ==================== About_table 转 HTML ====================

    def generate_about_table_html(self, text: str) -> str:
        """
        将About_table文本转换为HTML表格
        支持四种格式:
        1. 只有【】: 【标题】内容 → 双列
        2. 只有:: 标题: 内容 → 双列
        3. 有【】也有:: 【标题】: 内容 → 双列
        4. 什么都没有: 纯文本 → 单列
        """
        if not text:
            return ""

        # 如果已经是HTML，直接返回
        if text.strip().startswith("<"):
            return text

        # 先合并单独成行的冒号
        text = self._merge_lonely_colons(text)

        lines = text.split("\n")
        rows = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            title = None
            desc = None

            # 模式3: 【标题】: 内容 或 [Title]: 内容 或 (标题): 内容
            # 修正后的正则：匹配【】或[]或()包围的标题，后跟冒号
            match = re.match(r'[【\[(]([^【\[】\])]+?)[】\])][:：]\s*(.+)', line)
            if match:
                title = match.group(1).strip()
                desc = match.group(2).strip()
            else:
                # 模式1: 【标题】内容 或 [Title] Content（有括号无冒号）
                match = re.match(r'[【\[(]([^【\[】\])]+?)[】\]]\s*(.+)', line)
                if match:
                    title = match.group(1).strip()
                    desc = match.group(2).strip()
                else:
                    # 模式2: 标题: 内容（无括号，有冒号）
                    # 确保标题部分不包含括号
                    match = re.match(r'^([^:：【\[】\])]+?)[:：]\s*(.+)', line)
                    if match:
                        title = match.group(1).strip()
                        desc = match.group(2).strip()
                    else:
                        # 模式4: 纯文本 → 单列
                        title = None
                        desc = line

            if title and desc:
                rows.append(("double", title, desc))
            else:
                rows.append(("single", None, desc))

        if not rows:
            return text

        # 生成HTML（保持不变）
        html_rows = []
        for row_type, title, desc in rows:
            desc_html = desc.replace('\n', '<br>').replace('\r', '')

            if row_type == "double":
                row_html = f"""      <tr style="border-bottom:1px solid #e8e8e8;">
            <td style="width:25%;padding:12px 16px;background:#f8f9fa;font-weight:600;color:#2d3748;vertical-align:top;border-right:2px solid #e0e0e0;font-size:14px;">{title}</td>
            <td style="width:75%;padding:12px 16px;color:#4a5568;line-height:1.5;vertical-align:top;font-size:14px;">{desc_html}</td>
          </tr>"""
            else:
                row_html = f"""      <tr style="border-bottom:1px solid #e8e8e8;">
            <td colspan="2" style="width:100%;padding:12px 16px;color:#4a5568;line-height:1.5;vertical-align:top;font-size:14px;">{desc_html}</td>
          </tr>"""
            html_rows.append(row_html)

        html = f"""<table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;margin:10px 0;background:white;border-radius:6px;overflow:hidden;">
      <tbody>
    {chr(10).join(html_rows)}
      </tbody>
    </table>"""
        return html

    # ==================== item_specifics 转 HTML ====================

    def parse_item_specifics(self, text: str) -> dict:
        """解析item_specifics为字典"""
        if not text:
            return {}

        text = str(text).strip()

        # 先合并单独成行的冒号（处理 key:\nvalue 情况）
        text = self._merge_lonely_colons(text)

        # 尝试解析JSON
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
            elif isinstance(data, list) and len(data) > 0:
                return data[0] if isinstance(data[0], dict) else {}
        except:
            pass

        # 解析 key: value 格式
        result = {}
        for line in text.split('\n'):
            line = line.strip()
            if ':' in line:
                key, value = line.split(':', 1)
                result[key.strip()] = value.strip()

        return result

    def generate_technical_details_html(self, data: dict) -> str:
        """生成 Technical Details HTML 表格"""
        if not data:
            return ""

        # 标准字段顺序
        standard_order = [
            "Manufacturer",
            "Brand",
            "Model",
            "Item Weight",
            "Package Dimensions",
            "Item model number",
            "Exterior",
            "Manufacturer Part Number",
            "OEM Part Number"
        ]

        # 按标准顺序排序
        ordered_items = []
        remaining = dict(data)

        for key in standard_order:
            if key in remaining:
                ordered_items.append((key, remaining.pop(key)))

        # 添加剩余字段
        for key, value in remaining.items():
            ordered_items.append((key, value))

        if not ordered_items:
            return ""

        # 生成HTML行
        rows_html = []
        for key, value in ordered_items:
            clean_value = str(value).replace('\u200e', '').replace('\u200f', '').strip()
            # 值中的换行符转换为<br>
            clean_value = clean_value.replace('\n', '<br>')
            row = f"""        <tr style="border-bottom: 1px solid #e7e7e7;">
            <th style="color: #333333; font-size: 14px; font-weight: 700; text-align: left; padding: 7.5px 56.7px 7.5px 16px; width: auto; vertical-align: top; white-space: nowrap;">{key}</th>
            <td style="font-size: 14px; padding: 7.5px 16px 7.5px 0; vertical-align: top;">{clean_value}</td>
        </tr>"""
            rows_html.append(row)

        # 第一行加上顶部边框
        if rows_html:
            rows_html[0] = rows_html[0].replace(
                'border-bottom: 1px solid #e7e7e7;">',
                'border-top: 1px solid #e7e7e7; border-bottom: 1px solid #e7e7e7;">'
            )

        html = f"""<div style="font-size: 24px; font-weight: 700; padding: 10px 0; margin-bottom: 8px;">Technical Details</div>
<table style="border-collapse: collapse; width: 600px;">
    <tbody>
{chr(10).join(rows_html)}
    </tbody>
</table>"""

        return html

    # ==================== 主流程 ====================

    def run(self) -> None:
        """主处理流程"""
        print(f"===== HTML格式化：清洗非法字符 + 转HTML表格 =====")
        print(f"[1/5] 读取源文件: {self.src_file}")

        df = pd.read_excel(self.src_file)
        print(f"      成功读取 {len(df)} 行数据")
        print(f"      可用列: {list(df.columns)}")

        # 1. 清洗 About_table
        if 'About_table' in df.columns:
            print(f"[2/5] 清洗 About_table 列非法字符...")
            df['About_table'] = df['About_table'].apply(self.clean_text)
            # 合并单独成行的冒号
            df['About_table'] = df['About_table'].apply(self._merge_lonely_colons)
            print("      清洗完成（含冒号合并）")
        else:
            print(f"⚠️  警告: 未找到 'About_table' 列")

        # 2. 清洗 description_from_the_seller（只清洗，不转HTML）
        if 'description_from_the_seller' in df.columns:
            print(f"[3/5] 清洗 description_from_the_seller 列非法字符...")
            df['description_from_the_seller'] = df['description_from_the_seller'].apply(self.clean_text)
            # 合并单独成行的冒号
            df['description_from_the_seller'] = df['description_from_the_seller'].apply(self._merge_lonely_colons)
            print("      清洗完成（保持纯文本，不转HTML，含冒号合并）")
        else:
            print(f"⚠️  警告: 未找到 'description_from_the_seller' 列")

        # 3. About_table 转 HTML
        if 'About_table' in df.columns:
            print(f"[4/5] 转换 About_table 为 HTML 表格...")
            df['About_table'] = df['About_table'].apply(self.generate_about_table_html)
            print("      转换完成")

        # 4. item_specifics 转 HTML（先清洗再转换）
        item_col = None
        for col in ['item_specifics', 'Item Specifics', 'item specifics']:
            if col in df.columns:
                item_col = col
                break

        if item_col:
            print(f"[5/5] 清洗并转换 {item_col} 为 HTML 表格...")
            # 先清洗
            df[item_col] = df[item_col].apply(self.clean_text)
            # 合并单独成行的冒号（在parse_item_specifics内部已处理，这里也做一遍保险）
            df[item_col] = df[item_col].apply(self._merge_lonely_colons)
            # 再转HTML
            df[item_col] = df[item_col].apply(
                lambda x: self.generate_technical_details_html(self.parse_item_specifics(x))
            )
            # 统一列名
            if item_col != 'item_specifics':
                df = df.rename(columns={item_col: 'item_specifics'})
            print("      转换完成")
        else:
            print(f"⚠️  警告: 未找到 item_specifics 列")

        # 保存
        print(f"\n保存到: {self.dst_file}")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)
        print(f"✅ HTML格式化完成！")


def main():
    """供 main(1).py 调用的入口"""
    formatter = HTMLFormatter()
    formatter.run()


if __name__ == "__main__":
    main()