from __future__ import annotations

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from data.config import Config


class MagentoFormatter:
    """将源数据转换为Magento导入格式"""

    # 全角到半角符号映射表
    FULLWIDTH_TO_HALF = str.maketrans({
        # 标点符号
        '。': '.',  # U+3002 句号
        '，': ',',  # U+FF0C 逗号
        '、': ',',  # U+3001 顿号→逗号
        '；': ';',  # U+FF1B 分号
        '：': ':',  # U+FF1A 冒号
        '？': '?',  # U+FF1F 问号
        '！': '!',  # U+FF01 感叹号
        '…': '...',  # U+2026 省略号→三个点
        '—': '-',  # U+2014 Em Dash
        '–': '-',  # U+2013 En Dash
        '‐': '-',  # U+2010 Hyphen
        '‑': '-',  # U+2011 Non-breaking hyphen
        '‒': '-',  # U+2012 Figure dash
        '―': '-',  # U+2015 Horizontal bar
        '−': '-',  # U+2212 Minus sign
        '－': '-',  # U+FF0D 全角连字符
        '·': '.',  # U+00B7 中间点→句点
        '•': '.',  # U+2022 项目符号→句点
        '‧': '.',  # U+2027 连字点→句点

        # 括号类
        '（': '(',  # U+FF08 左圆括号
        '）': ')',  # U+FF09 右圆括号
        '【': '[',  # U+3010 左方括号
        '】': ']',  # U+3011 右方括号
        '［': '[',  # U+FF3B 全角左方括号
        '］': ']',  # U+FF3D 全角右方括号
        '｛': '{',  # U+FF5B 左花括号
        '｝': '}',  # U+FF5D 右花括号
        '《': '<',  # U+300A 左书名号→小于号
        '》': '>',  # U+300B 右书名号→大于号
        '〈': '<',  # U+3008 左单书名号
        '〉': '>',  # U+3009 右单书名号

        # 引号类
        '「': '"',  # U+300C 左单书名号→双引号
        '」': '"',  # U+300D 右单书名号→双引号
        '『': '"',  # U+300E 左双书名号→双引号
        '』': '"',  # U+300F 右双书名号→双引号
        '“': '"',  # U+201C 左双引号
        '”': '"',  # U+201D 右双引号
        '‘': "'",  # U+2018 左单引号
        '’': "'",  # U+2019 右单引号
        '＂': '"',  # U+FF02 全角双引号
        '＇': "'",  # U+FF07 全角单引号
        '◆': '.',
        # 空格
        '　': ' ',  # U+3000 全角空格→半角空格

        # 数学符号
        '＋': '+',  # U+FF0B 全角加号
        '＊': '*',  # U+FF0A 全角星号
        '／': '/',  # U+FF0F 全角斜杠
        '＝': '=',  # U+FF1D 全角等号
        '％': '%',  # U+FF05 全角百分号
        '＃': '#',  # U+FF03 全角井号
        '＆': '&',  # U+FF06 全角and
        '＠': '@',  # U+FF20 全角at
        '＄': '$',  # U+FF04 全角美元
        '￥': 'Y',  # U+FFE5 人民币→Y
        '￠': 'c',  # U+FFE0 分币符号→c
        '￡': 'L',  # U+FFE1 英镑→L

        # 数字 0-9
        '０': '0', '１': '1', '２': '2', '３': '3', '４': '4',
        '５': '5', '６': '6', '７': '7', '８': '8', '９': '9',

        # 大写字母 A-Z
        'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E',
        'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
        'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O',
        'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
        'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y',
        'Ｚ': 'Z',

        # 小写字母 a-z
        'ａ': 'a', 'ｂ': 'b', 'ｃ': 'c', 'ｄ': 'd', 'ｅ': 'e',
        'ｆ': 'f', 'ｇ': 'g', 'ｈ': 'h', 'ｉ': 'i', 'ｊ': 'j',
        'ｋ': 'k', 'ｌ': 'l', 'ｍ': 'm', 'ｎ': 'n', 'ｏ': 'o',
        'ｐ': 'p', 'ｑ': 'q', 'ｒ': 'r', 'ｓ': 's', 'ｔ': 't',
        'ｕ': 'u', 'ｖ': 'v', 'ｗ': 'w', 'ｘ': 'x', 'ｙ': 'y',
        'ｚ': 'z',
    })

    # 固定值配置
    FIXED_VALUES = {
        'attribute_set_code': 'Default',
        'product_type': 'simple',
        'category_ids': '9,206',
        'product_websites': 'base',
        'weight': 10,
        'product_online': 1,
        'tax_class_name': 'Taxable Goods',
        'visibility': 'Catalog, Search',
        'call_for_price': 0,
        'quote': 100,
        'display_product_options_in': 'Block after Info Column',
        'gift_message_available': 'Use config',
        'qty': 10000,
        'out_of_stock_qty': 0,
        'use_config_min_qty': 1,
        'is_qty_decimal': 0,
        'allow_backorders': 0,
        'use_config_backorders': 1,
        'min_sale_qty': 1,
        'use_config_min_sale_qty': 0,
        'max_sale_qty': 1000,
        'use_config_max_sale_qty': 1,
        'is_in_stock': 1,
        'notify_on_stock_below': 1,
        'use_config_notify_stock_qty': 1,
        'manage_stock': 1,
        'use_config_manage_stock': 1,
        'use_config_qty_increments': 1,
        'qty_increments': 1,
        'use_config_enable_qty_inc': 1,
        'enable_qty_increments': 1,
        'is_decimal_divided': 0,
        'website_id': 0,
    }

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_name = self.config.product_user_2

        self.src_file = Path(f"data/{self.product_name}/{self.product_name}_已格式化.xlsx")
        self.dst_file = Path(f"data/{self.product_name}/{self.product_name}_已格式化.csv")
        self.sku_file = Path("data/sq.txt")

        # 目标列（不包含 add 列，因为会被处理掉）
        self.target_columns = [
            'sku', 'position', 'store_view_code', 'attribute_set_code', 'product_type',
            'category_ids', 'product_websites', 'name', 'description', 'short_description',
            'weight', 'product_online', 'tax_class_name', 'visibility', 'price',
            'call_for_price', 'cost', 'model', 'quote', 'special_price',
            'special_price_from_date', 'special_price_to_date', 'url_key', 'meta_title',
            'meta_keyword', 'meta_description', 'image', 'image_label', 'small_image',
            'small_image_label', 'thumbnail', 'thumbnail_label', 'swatch_image',
            'swatch_image_label', 'created_at', 'updated_at', 'new_from_date', 'new_to_date',
            'display_product_options_in', 'map_price', 'msrp_price', 'map_enabled',
            'gift_message_available', 'custom_design', 'custom_design_from', 'custom_design_to',
            'custom_layout_update', 'page_layout', 'product_options_container',
            'msrp_display_actual_price_type', 'country_of_manufacture', 'additional_attributes',
            'qty', 'out_of_stock_qty', 'use_config_min_qty', 'is_qty_decimal', 'allow_backorders',
            'use_config_backorders', 'min_sale_qty', 'use_config_min_sale_qty', 'max_sale_qty',
            'use_config_max_sale_qty', 'is_in_stock', 'notify_on_stock_below',
            'use_config_notify_stock_qty', 'manage_stock', 'use_config_manage_stock',
            'use_config_qty_increments', 'qty_increments', 'use_config_enable_qty_inc',
            'enable_qty_increments', 'is_decimal_divided', 'website_id', 're_skus', 're_position',
            'crosssell_skus', 'crosssell_position', 'upsell_skus', 'upsell_position',
            'media_gallery', 'media_label', 'hide_from_product_page', 'custom_options',
            'bundle_price_type', 'bundle_sku_type', 'bundle_price_view', 'bundle_weight_type',
            'bundle_values', 'bundle_shipment_type', 'configurable_variations',
            'configurable_variation_labels', 'associated_skus'
        ]

        # 统计信息
        self.replace_stats = {
            'total_replaced': 0,
            'rows_affected': 0,
            'fields_checked': ['标题', '图片本地地址', '价格', '产品描述', '短描述', 'OEM'],
            'details': []
        }

        # 品类名称（手动输入）
        self.category_name = self.input_category_name()

    def input_category_name(self) -> str:
        """手动输入品类名称"""
        print(f"\n{'=' * 60}")
        print("请输入品类名称（例如：Engine、Brake、Suspension等）")
        print(f"{'=' * 60}")

        while True:
            name = input("品类名称: ").strip()
            if name:
                return name
            print("品类名称不能为空，请重新输入！")

    def read_sku_seed(self) -> int:
        """从sq.txt读取当前SKU序号"""
        if not self.sku_file.exists():
            raise FileNotFoundError(f"SKU文件不存在: {self.sku_file}")

        content = self.sku_file.read_text().strip()
        match = re.search(r'carpart-(\d+)', content)
        if not match:
            raise ValueError(f"SKU文件格式错误: {content}")

        return int(match.group(1))

    def write_sku_seed(self, last_used_number: int) -> None:
        """将最后一个使用的SKU序号写入文件"""
        next_number = last_used_number + 1
        self.sku_file.parent.mkdir(parents=True, exist_ok=True)
        self.sku_file.write_text(f"carpart-{next_number}")

    def replace_fullwidth_chars(self, text: str, row_idx: int = -1, field_name: str = '') -> str:
        """将全角符号替换为半角符号，并统计数量"""
        if not text or not isinstance(text, str):
            return text if text else ''

        # 执行替换
        cleaned = text.translate(self.FULLWIDTH_TO_HALF)

        # 统计替换数量
        replaced_count = 0
        replaced_chars = []

        for char in set(text):
            if char in self.FULLWIDTH_TO_HALF:
                count = text.count(char)
                replaced_count += count
                replaced_chars.append(f"{char}→{self.FULLWIDTH_TO_HALF[char]}")

        if replaced_count > 0:
            self.replace_stats['total_replaced'] += replaced_count

            if row_idx != -1:
                if not hasattr(self, '_affected_rows'):
                    self._affected_rows = set()
                self._affected_rows.add(row_idx)

            # 记录详细信息（前10个示例）
            if len(self.replace_stats['details']) < 10 and field_name:
                self.replace_stats['details'].append({
                    'row': row_idx,
                    'field': field_name,
                    'original': text[:50] + '...' if len(text) > 50 else text,
                    'result': cleaned[:50] + '...' if len(cleaned) > 50 else cleaned,
                    'changes': replaced_chars[:5],
                    'count': replaced_count
                })

        return cleaned

    def get_affected_rows_count(self) -> int:
        """获取受影响的行数"""
        return len(getattr(self, '_affected_rows', set()))

    def extract_first_image(self, image_paths: str, row_idx: int) -> str:
        """提取第一个分号前的图片路径，替换全角符号"""
        cleaned = self.replace_fullwidth_chars(image_paths, row_idx, '图片本地地址')
        if ';' in cleaned:
            return cleaned.split(';')[0]
        return cleaned

    def generate_meta_keyword(self, title: str) -> str:
        """生成meta_keyword：标题最后一个数字+1"""
        if not title:
            return ''

        numbers = re.findall(r'\d+', title)
        if not numbers:
            return ''

        last_number = int(numbers[-1])
        new_number = last_number + 1
        last_num_str = numbers[-1]
        last_index = title.rfind(last_num_str)

        if last_index != -1:
            return title[:last_index] + str(new_number) + title[last_index + len(last_num_str):]
        return ''

    def get_current_time(self) -> str:
        """获取当前时间，格式 2026/3/2 10:52"""
        now = datetime.now()
        return f"{now.year}/{now.month}/{now.day} {now.hour}:{now.minute:02d}"

    def generate_add_column(self, oem_value: str, row_idx: int) -> str:
        """生成 add 列的值：品类名 + '-' + OEM值"""
        if not oem_value or pd.isna(oem_value):
            # 如果OEM列为空，只返回品类名
            return self.category_name

        # 替换OEM值中的全角符号
        cleaned_oem = self.replace_fullwidth_chars(str(oem_value), row_idx, 'OEM')

        # 连接品类名和OEM值
        return f"{self.category_name}-{cleaned_oem}"

    def sanitize_url_key(self, text: str) -> str:
        """
        清理URL key，使其符合SEO规范：
        1. 将 & 符号转换为 -
        2. 将所有大写字母转换为小写
        3. 移除特殊字符
        4. 规范化连字符
        """
        if not text:
            return ''

        # 1. 将 & 符号转换为连字符
        text = text.replace('&', '-')
        text = text.replace('＆', '-')  # 全角&符号

        # 2. 将所有字母转换为小写
        text = text.lower()

        # 3. 将空格替换为连字符
        text = text.replace(' ', '-')

        # 4. 移除特殊字符（只保留字母、数字、连字符、下划线）
        text = re.sub(r'[^a-z0-9\-_]', '', text)

        # 5. 将多个连续连字符替换为单个
        text = re.sub(r'-+', '-', text)

        # 6. 移除首尾连字符
        text = text.strip('-')

        return text

    def process_row(self, row: pd.Series, sku_number: int, row_idx: int) -> dict:
        """处理单行数据"""
        # 替换各字段全角符号（带统计）
        clean_title = self.replace_fullwidth_chars(str(row.get('标题', '')), row_idx, '标题')
        description = self.replace_fullwidth_chars(str(row.get('产品描述', '')), row_idx, '产品描述')
        short_description = self.replace_fullwidth_chars(str(row.get('短描述', '')), row_idx, '短描述')
        price_str = self.replace_fullwidth_chars(str(row.get('价格', '')), row_idx, '价格')

        # 获取OEM值并生成add列的值（用于url_key）
        oem_value = row.get('OEM', '')
        add_value = self.generate_add_column(oem_value, row_idx)

        # 生成 url_key：使用 add_value.html，并清理特殊字符
        cleaned_url = self.sanitize_url_key(add_value)
        url_key = f"{cleaned_url}.html" if cleaned_url else "product.html"

        # 图片处理
        raw_image_paths = str(row.get('图片本地地址', ''))
        first_image = self.extract_first_image(raw_image_paths, row_idx)
        clean_image_paths = self.replace_fullwidth_chars(raw_image_paths, row_idx, '图片本地地址')

        sku = f"carpart-{sku_number}"
        position = sku_number
        current_time = self.get_current_time()

        return {
            'sku': sku,
            'position': position,
            'store_view_code': '',
            'attribute_set_code': self.FIXED_VALUES['attribute_set_code'],
            'product_type': self.FIXED_VALUES['product_type'],
            'category_ids': self.FIXED_VALUES['category_ids'],
            'product_websites': self.FIXED_VALUES['product_websites'],
            'name': clean_title,
            'description': description,
            'short_description': short_description,
            'weight': self.FIXED_VALUES['weight'],
            'product_online': self.FIXED_VALUES['product_online'],
            'tax_class_name': self.FIXED_VALUES['tax_class_name'],
            'visibility': self.FIXED_VALUES['visibility'],
            'price': price_str,
            'call_for_price': self.FIXED_VALUES['call_for_price'],
            'cost': '',
            'model': sku,
            'quote': self.FIXED_VALUES['quote'],
            'special_price': '',
            'special_price_from_date': '',
            'special_price_to_date': '',
            'url_key': url_key,  # 使用清理后的 add_value.html
            'meta_title': clean_title,
            'meta_keyword': self.generate_meta_keyword(clean_title),
            'meta_description': '',
            'image': first_image,
            'image_label': '',
            'small_image': first_image,
            'small_image_label': '',
            'thumbnail': first_image,
            'thumbnail_label': '',
            'swatch_image': first_image,
            'swatch_image_label': '',
            'created_at': current_time,
            'updated_at': current_time,
            'new_from_date': '',
            'new_to_date': '',
            'display_product_options_in': self.FIXED_VALUES['display_product_options_in'],
            'map_price': '',
            'msrp_price': '',
            'map_enabled': '',
            'gift_message_available': self.FIXED_VALUES['gift_message_available'],
            'custom_design': '',
            'custom_design_from': '',
            'custom_design_to': '',
            'custom_layout_update': '',
            'page_layout': '',
            'product_options_container': '',
            'msrp_display_actual_price_type': '',
            'country_of_manufacture': '',
            'additional_attributes': '',
            'qty': self.FIXED_VALUES['qty'],
            'out_of_stock_qty': self.FIXED_VALUES['out_of_stock_qty'],
            'use_config_min_qty': self.FIXED_VALUES['use_config_min_qty'],
            'is_qty_decimal': self.FIXED_VALUES['is_qty_decimal'],
            'allow_backorders': self.FIXED_VALUES['allow_backorders'],
            'use_config_backorders': self.FIXED_VALUES['use_config_backorders'],
            'min_sale_qty': self.FIXED_VALUES['min_sale_qty'],
            'use_config_min_sale_qty': self.FIXED_VALUES['use_config_min_sale_qty'],
            'max_sale_qty': self.FIXED_VALUES['max_sale_qty'],
            'use_config_max_sale_qty': self.FIXED_VALUES['use_config_max_sale_qty'],
            'is_in_stock': self.FIXED_VALUES['is_in_stock'],
            'notify_on_stock_below': self.FIXED_VALUES['notify_on_stock_below'],
            'use_config_notify_stock_qty': self.FIXED_VALUES['use_config_notify_stock_qty'],
            'manage_stock': self.FIXED_VALUES['manage_stock'],
            'use_config_manage_stock': self.FIXED_VALUES['use_config_manage_stock'],
            'use_config_qty_increments': self.FIXED_VALUES['use_config_qty_increments'],
            'qty_increments': self.FIXED_VALUES['qty_increments'],
            'use_config_enable_qty_inc': self.FIXED_VALUES['use_config_enable_qty_inc'],
            'enable_qty_increments': self.FIXED_VALUES['enable_qty_increments'],
            'is_decimal_divided': self.FIXED_VALUES['is_decimal_divided'],
            'website_id': self.FIXED_VALUES['website_id'],
            're_skus': sku,
            're_position': '',
            'crosssell_skus': '',
            'crosssell_position': '',
            'upsell_skus': '',
            'upsell_position': '',
            'media_gallery': clean_image_paths,
            'media_label': '',
            'hide_from_product_page': '',
            'custom_options': '',
            'bundle_price_type': '',
            'bundle_sku_type': '',
            'bundle_price_view': '',
            'bundle_weight_type': '',
            'bundle_values': '',
            'bundle_shipment_type': '',
            'configurable_variations': '',
            'configurable_variation_labels': '',
            'associated_skus': '',
            # 注意：不再包含 'add' 列
        }

    def print_replace_report(self):
        """打印全角符号替换报告"""
        print(f"\n{'=' * 60}")
        print("📊 全角符号替换统计报告")
        print(f"{'=' * 60}")
        print(f"  品类名称: {self.category_name}")
        print(f"  检查字段: {', '.join(self.replace_stats['fields_checked'])}")
        print(f"  总行数: {self.replace_stats.get('total_rows', 0)}")
        print(f"  受影响行数: {self.get_affected_rows_count()}")
        print(f"  替换字符总数: {self.replace_stats['total_replaced']} 个")
        print(
            f"  替换比例: {self.get_affected_rows_count() / max(self.replace_stats.get('total_rows', 1), 1) * 100:.1f}%")

        if self.replace_stats['details']:
            print(f"\n  替换示例 (前{len(self.replace_stats['details'])}条):")
            for detail in self.replace_stats['details']:
                changes_str = ', '.join(detail['changes'])
                print(f"    行{detail['row']}[{detail['field']}]: 替换 {detail['count']}个字符")
                print(f"      原文: {detail['original']}")
                print(f"      结果: {detail['result']}")
                print(f"      映射: {changes_str}")
                print()
        print(f"{'=' * 60}")

    def run(self) -> None:
        """主处理流程"""
        print(f"\n{'=' * 60}")
        print(f"===== Magento格式转换器 =====")
        print(f"品类: {self.category_name}")
        print(f"{'=' * 60}")

        print(f"\n[1/7] 读取SKU种子文件: {self.sku_file}")
        sku_start = self.read_sku_seed()
        print(f"      起始SKU: carpart-{sku_start}")

        print(f"\n[2/7] 读取源文件: {self.src_file}")
        if not self.src_file.exists():
            print(f"❌ 错误: 文件不存在 - {self.src_file}")
            return

        df = pd.read_excel(self.src_file)
        original_count = len(df)
        self.replace_stats['total_rows'] = original_count
        print(f"      成功读取 {original_count} 行数据")

        # 检查必需列
        required_cols = ['标题', '图片本地地址', '价格', '产品描述', '短描述']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ 错误: 缺少必需列: {missing_cols}")
            return

        # 检查OEM列（如果没有则创建空列）
        if 'OEM' not in df.columns:
            print(f"⚠️  警告: 源文件中没有'OEM'列，将使用空值")
            df['OEM'] = ''

        print(f"\n[3/7] 转换数据格式并替换全角符号...")
        results = []
        for idx, row in df.iterrows():
            sku_number = sku_start + idx
            processed = self.process_row(row, sku_number, idx)
            results.append(processed)

            if (idx + 1) % 100 == 0 or idx == original_count - 1:
                print(f"      已处理 {idx + 1}/{original_count} 行")

        # 更新受影响行数统计
        self.replace_stats['rows_affected'] = self.get_affected_rows_count()

        print(f"\n[4/7] 生成结果数据...")
        result_df = pd.DataFrame(results, columns=self.target_columns)

        print(f"\n[5/7] 显示 url_key 示例数据（前5行）:")
        for i in range(min(5, len(result_df))):
            print(f"      行{i + 1}: {result_df.iloc[i]['url_key']}")

        print(f"\n[6/7] 保存CSV文件: {self.dst_file}")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(self.dst_file, index=False, encoding='utf-8')
        print(f"      成功保存 {len(result_df)} 行数据")
        print(f"      注意: add列已处理并删除，内容已用于生成 url_key")

        print(f"\n[7/7] 更新SKU种子文件...")
        last_used = sku_start + original_count - 1
        self.write_sku_seed(last_used)
        print(f"      最后使用SKU: carpart-{last_used}")

        # 打印替换报告
        self.print_replace_report()

        print(f"\n✅ 处理完成！")
        print(f"   输出文件: {self.dst_file}")
        print(f"   url_key处理规则:")
        print(f"     - & 符号转换为 -")
        print(f"     - 所有大写字母转换为小写")
        print(f"     - 空格转换为 -")
        print(f"     - 特殊字符被移除")
        print(f"     - 多个连符合并为一个")
        print(f"     - 格式: {self.category_name.lower()}-[清理后的OEM值].html")


def main():
    """供 main(1).py 调用的入口"""
    formatter = MagentoFormatter()
    formatter.run()


if __name__ == "__main__":
    main()