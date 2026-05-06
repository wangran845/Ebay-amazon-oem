from __future__ import annotations

import re
import json
from pathlib import Path

import pandas as pd
from data.config import Config


class FinalFormatter:
    """最终格式化：合并描述、转HTML、重组列"""

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_OEM去重后.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_中间格式化.xlsx")

    # ==================== 文本处理工具 ====================

    def remove_duplicates_between_columns(self, about_text: str, desc_text: str) -> str:
        """
        删除description_from_the_seller中与About_table重复的内容
        按换行符分割成元素，去除重复后重新组合
        """
        if pd.isna(desc_text) or not str(desc_text).strip():
            return desc_text
        if pd.isna(about_text) or not str(about_text).strip():
            return desc_text

        # 分割成元素（按换行符）
        about_lines = [line.strip() for line in str(about_text).split('\n') if line.strip()]
        desc_lines = [line.strip() for line in str(desc_text).split('\n') if line.strip()]

        # 创建About_table的查找集合（标准化后用于比较）
        about_set = set()
        for line in about_lines:
            # 标准化：去除多余空格、转小写、去除标点
            normalized = re.sub(r'[^\w\s]', '', line.lower())
            normalized = re.sub(r'\s+', ' ', normalized).strip()
            if normalized:
                about_set.add(normalized)

        # 过滤desc_lines，保留不重复的内容
        unique_desc_lines = []
        for line in desc_lines:
            normalized = re.sub(r'[^\w\s]', '', line.lower())
            normalized = re.sub(r'\s+', ' ', normalized).strip()

            # 如果标准化后的内容不在about_set中，保留原行
            if normalized not in about_set:
                unique_desc_lines.append(line)

        # 重新组合
        return '\n'.join(unique_desc_lines) if unique_desc_lines else ""

    # ==================== About_table 转 HTML ====================

    def generate_about_table_html(self, text: str) -> str:
        """
        将About_table文本转换为HTML表格
        双列：标题25%灰色背景，内容75%白色背景
        """
        if not text:
            return ""

        if text.strip().startswith("<"):
            return text

        lines = text.split("\n")
        rows = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            title = None
            desc = None

            # 模式3: 【标题】: 内容 或 [标题]: 内容 或 (标题): 内容
            # 修正：用 (?:...) 分组，不是字符集 [...]
            match = re.match(r'(?:【(.+?)】|\[(.+?)\]|\((.+?)\))[:：]\s*(.+)', line)
            if match:
                # 三个括号类型只有一个能匹配，找出是哪个
                title = match.group(1) or match.group(2) or match.group(3)
                desc = match.group(4)
            else:
                # 模式1: 【标题】内容 或 [标题] 内容 或 (标题) 内容（无冒号）
                match = re.match(r'(?:【(.+?)】|\[(.+?)\]|\((.+?)\))\s*(.+)', line)
                if match:
                    title = match.group(1) or match.group(2) or match.group(3)
                    desc = match.group(4)
                else:
                    # 模式2: 标题: 内容（无括号）
                    match = re.match(r'^([^:：【\[\]（）]+?)[:：]\s*(.+)', line)
                    if match:
                        title = match.group(1).strip()
                        desc = match.group(2).strip()
                    else:
                        # 模式4: 纯文本
                        title = None
                        desc = line

            if title and desc:
                rows.append(("double", title.strip(), desc.strip()))
            else:
                rows.append(("single", None, desc))

        if not rows:
            return text

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

    # ==================== description_from_the_seller 装饰 ====================

    def format_description_with_style(self, text: str) -> str:
        """
        将description_from_the_seller转换为带装饰的HTML
        每行加上<p>标签，整体用<div>包裹，并添加样式
        """
        if pd.isna(text) or not str(text).strip():
            return ''

        text = str(text).strip()

        # 如果已经是HTML（包含<div>或<table>），直接返回
        if '<div' in text.lower() or '<table' in text.lower():
            return text

        # 按换行符分割
        lines = text.split("\n")

        # 每行加上 <p> 标签，并添加样式
        p_lines = []
        for line in lines:
            line = line.strip()
            if line:
                # 🔥 关键修复：将行内的换行符转换为<br>，确保内容换行
                line_html = line.replace('\n', '<br>').replace('\r', '')
                p_lines.append(f'<p style="margin:0 0 10px 0;padding:0;line-height:1.6;color:#4a5568;font-size:14px;">{line_html}</p>')

        # 合并并用 div 包裹，添加整体样式
        inner_html = "\n".join(p_lines)
        result = f""""""

        return result

    # ==================== item_specifics 转 HTML ====================

    def parse_item_specifics(self, text: str) -> dict:
        """解析item_specifics为字典"""
        if not text:
            return {}

        text = str(text).strip()

        # 如果已经是HTML，不需要解析
        if text.startswith("<"):
            return {}

        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                return data[0] if isinstance(data[0], dict) else {}
        except:
            pass

        result = {}
        for line in text.split("\n"):
            line = line.strip()
            if ":" in line:
                key, value = line.split(":", 1)
                result[key.strip()] = value.strip()

        return result

    def generate_technical_details_html(self, data: dict) -> str:
        """生成 Technical Details HTML 表格"""
        if not data:
            return ""

        standard_order = [
            "Manufacturer", "Brand", "Model", "Item Weight",
            "Package Dimensions", "Item model number", "Exterior",
            "Manufacturer Part Number", "OEM Part Number"
        ]

        ordered_items = []
        remaining = dict(data)

        for key in standard_order:
            if key in remaining:
                ordered_items.append((key, remaining.pop(key)))

        for key, value in remaining.items():
            ordered_items.append((key, value))

        if not ordered_items:
            return ""

        rows_html = []
        for key, value in ordered_items:
            clean_value = str(value).replace("\u200e", "").replace("\u200f", "").strip()
            # 🔥 修复：值中的换行符转换为<br>
            clean_value = clean_value.replace('\n', '<br>')
            row = f"""        <tr style="border-bottom: 1px solid #e7e7e7;">
            <th style="color: #333333; font-size: 14px; font-weight: 700; text-align: left; padding: 7.5px 56.7px 7.5px 16px; width: auto; vertical-align: top; white-space: nowrap;">{key}</th>
            <td style="font-size: 14px; padding: 7.5px 16px 7.5px 0; vertical-align: top;">{clean_value}</td>
        </tr>"""
            rows_html.append(row)

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

    def process_item_specifics(self, text) -> str:
        """处理 item_specifics：如果是HTML直接返回，否则解析生成HTML"""
        if pd.isna(text) or not str(text).strip():
            return ""

        text_str = str(text).strip()

        # 如果已经是HTML，直接返回
        if text_str.startswith("<"):
            return text_str

        # 否则解析并生成HTML
        data = self.parse_item_specifics(text_str)
        return self.generate_technical_details_html(data)

    # ==================== 合并产品描述 ====================

    def merge_descriptions(self, about_html: str, desc_html: str) -> str:
        """
        合并 About_table 和 description_from_the_seller 的HTML
        上下排列，用换行分隔
        """
        about_html = str(about_html) if pd.notna(about_html) else ""
        desc_html = str(desc_html) if pd.notna(desc_html) else ""

        # 清理空白
        about_html = about_html.strip()
        desc_html = desc_html.strip()

        # 如果一方为空，返回另一方
        if not about_html:
            return desc_html
        if not desc_html:
            return about_html

        # 直接拼接，用换行分隔
        return f"{about_html}\n{desc_html}"

    # ==================== 主流程 ====================

    def run(self) -> None:
        """主处理流程"""
        print(f"===== 最终格式化 =====")
        print(f"[1/5] 读取源文件: {self.src_file}")

        if not self.src_file.exists():
            print(f"❌ 错误: 文件不存在 - {self.src_file}")
            return

        df = pd.read_excel(self.src_file)
        print(f"      成功读取 {len(df)} 行数据")

        # 查找列
        about_col = 'About_table' if 'About_table' in df.columns else None
        desc_col = 'description_from_the_seller' if 'description_from_the_seller' in df.columns else None
        item_col = None
        for col in ['item_specifics', 'Item Specifics', 'item specifics', '核心参数']:
            if col in df.columns:
                item_col = col
                break

        # 1. 先查重：删除description中与About重复的内容
        if about_col and desc_col:
            print(f"[2/5] 查重：删除 {desc_col} 中与 {about_col} 重复的内容...")
            df[desc_col] = df.apply(
                lambda row: self.remove_duplicates_between_columns(
                    row.get(about_col, ""),
                    row.get(desc_col, "")
                ),
                axis=1
            )
            print("      查重去重完成")
        else:
            print(f"[2/5] 跳过查重：缺少必要的列")

        # 2. About_table 转 HTML（25% / 75%）
        if about_col:
            print(f"[3/5] 转换 About_table 为 HTML 表格")
            df['About_table'] = df[about_col].apply(
                lambda x: x if pd.isna(x) or str(x).strip().startswith("<") else self.generate_about_table_html(str(x).strip())
            )
            print("      转换完成")

        # 3. description_from_the_seller 添加装饰
        if desc_col:
            print(f"[4/5] 装饰 description_from_the_seller...")
            df['description_from_the_seller'] = df[desc_col].apply(self.format_description_with_style)
            print("      装饰完成")

        # 4. item_specifics 转 HTML
        if item_col:
            print(f"[5/5] 转换 {item_col} 为 Technical Details HTML...")
            df['item_specifics'] = df[item_col].apply(self.process_item_specifics)
            print("      转换完成")

        # 5. 合并产品描述（About + description）
        print(f"\n合并产品描述...")
        df['产品描述'] = df.apply(
            lambda row: self.merge_descriptions(
                row.get('About_table', ''),
                row.get('description_from_the_seller', '')
            ),
            axis=1
        )
        print("      合并完成")

        # 6. 重组列
        print(f"\n重组列并保存...")
        new_df = pd.DataFrame({
            '标题': df.get('Title', ''),
            '图片本地地址': df.get('image_paths', ''),
            '价格': df.get('price', ''),
            '产品描述': df.get('产品描述', ''),
            '短描述': df.get('item_specifics', ''),
            '产品评论': df.get('seller_feedback', ''),
            'OEM': df.get('OME', ''),
            '源链接地址': df.get('URL', ''),
            '图片源链接': df.get('picture', ''),
            '产品类目': df.get('category', '')
        })

        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_excel(self.dst_file, index=False)
        print(f"✅ 已生成最终文件: {self.dst_file.resolve()}")


def main():
    """供 main(1).py 调用的入口"""
    formatter = FinalFormatter()
    formatter.run()


if __name__ == "__main__":
    main()