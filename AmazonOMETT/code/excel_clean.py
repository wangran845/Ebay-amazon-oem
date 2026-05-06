from __future__ import annotations

import re
import json
from pathlib import Path

import pandas as pd
from data.config import Config


class ExcelCleaner:
    """Excel初步清洗：清理描述 + 提取核心参数"""

    # 保留品牌白名单（写死在代码里）
    ALLOWED_BRANDS = {
        'Bosch', 'Denso', 'SKF', 'HELLA', 'ZF', 'Delphi',
        'Mahle', 'Valeo', 'Aisin', 'NGK', 'BorgWarner',
        'Schaeffler', 'NTK','Volkswagen','Audi','BMW','Mercedes-Benz','Porsche','MAN',
        'Scania','Smart','Toyota','Lexus','Honda','Nissan','Infiniti','Mazda','Subaru',
        'Suzuki','Isuzu','Hino','Ford','Lincoln','Chevrolet','GMC','Cadillac','Jeep','Hyundai',
        'Kia','Renault','Peugeot','Citroen','Ferrari','Lamborghini','Maserati','Fiat','Iveco',
        'Jaguar','Land Rover','Mini','Rolls-Royce','Bentley','Aston Martin','Volvo','Volvo Trucks',
        'Scania','Skoda','VW','HON','NIS','MAZ','Benz','GMC','Chrysler','Tesla','HOWO','Shacman',
        'CAMC','DAF','Western Star','Freightliner','Peterbilt','Hino','Hummer','AMC','Acura',
        'Opel','Skoda','Chrysler','Dodge','Tesla','TOYOTA','no branded','unbrand','without brand','Cummins',
        'GM GENUINE PARTS','General Motors','Volvo Penta','Acura','Genuine','HYUNDAI','NISSAN','GM Parts',
        'LEXUS','Land Rover','MINI COOPER','Mercedes Benz','Perkins','nobranded','none-branded','replacement for',
        'ホンダ(Honda)','日立(HITACHI)','HITACHI','Chrysler','OEM','AIRTEX','CHRYSLER','Saab','MAC'
    }

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_标题去重.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_初步删选.xlsx")

    def clean_description(self, text: str) -> str:
        """清理description_from_the_seller列的垃圾文本"""
        if pd.isna(text):
            return ""

        text = str(text)

        patterns = [
            r'Keyboard shortcut shift\+alt\+opt\+DShipping cost, delivery date, and order total \(including tax\) shown at checkout\.?',
            r'Color: \d+ pack\s*Make a Color selectionVideos',
            r'Help others learn more about this product by uploading a video!Upload your videoHelp others learn more about this product by uploading a video!',
            r'Help others learn more about this product by uploading a video!',
            r'Upload your video',
            r'Dear buyers, wecome to the store We provide a variety of high-quality products and intimate service to meet your diferentneeds, ifyou have any questions, pleasefel fre to tellus, we willserve you wholeheartedly, no matter when. your satisfaction is always our pursuit, I wish you a happy shopping!',
            r'Our commitment is to provide simple and reliable products to meet the needs of professional mechanics and DIY enthusiasts. Explore our store and find the exact parts you need to keep your vehicle running at its best.',
            r'installation is key, which is why we ensure that our list includes comprehensive, vehicle-specific compatibility information and detailed specifications to help you make the right choice with confidence.',
            r'Welcome to our auto parts store, your reliable partner in auto parts',
            r'We are professional sellers on , dedicated to providing a selection of high-quality auto parts and accessories. Our focus is on providing reliability and performance for your vehicle.',
            r'Every product in our catalogue, from engine components to suspension parts, focuses on durability, precise fit and seamless integration. We understand that correct',
            r'Hello friend, the logistics department has replied to us that during the delivery process, they found that your delivery address is invalid because the transportation time is about to expire. The logistics department has already signed for the product on your behalf. Please provide me with the correct delivery, and I will request the logistics department to resend your product to you. Please reply to me as soon as you see the email. thank!',
            r'Owners, look here! Lightweight, low wind resistance accessories area, without increasing the endurance burden, has both value and practicality, and protects your every green trip.',
            r'a premium European company for premium European vehicles. BMW, Mercedes, VAG, Volvo, Mini.',
            r'Saleri is a leading company in the design, development and production of water pumps and cooling systems for the German OEM’s.',
            r'Saleri is leading the way with new cooling technology for electric vehicles and auxiliary cooling systems.',
            r'a premium European company for premium European vehicles. BMW, Mercedes, VAG, Volvo, Mini. is a leading company in the design, development and production of water pumps and cooling systems for the German OEM’s. is leading the way with new cooling technology for electric vehicles and auxiliary cooling systems. the quality of products is confirmed by their development partnership with the most prestigious automotive companies, including BMW, VW, MERCEDES, FCA, FERRARI, GM and ASTON MARTIN. has implemented a highly automated production system, based on principles of modularity and flexibility. This enables it to produce to extremely high quality and reliability standards.',
            r'Hello, customer! This store operates car water pumps Our store takes integrity as the foundation, quality as the standard, and always adheres to the service concept of winning reputation with quality, allowing customers to enjoy comprehensive and thoughtful service. If necessary, please contact me! Thank you',
            r'Dear friends, welcome to our shop! We are a professional auto parts store. I hope our products can help you, and wish you a happy shopping!',
            r'Welcome to our store and buy our car reversing camera! Clear vision, easy reversing, make your trip more secure.',
            r'Welcome to my auto parts specialty store!',
            r'Whether you are the "detail control" of your car or the "practicality" pursuing efficient maintenance, we can find high-quality automotive parts that are suitable for you here - from daily maintenance filters and brake pads to performance enhancing modified parts. We carefully select each product to ensure precise adaptation and reliable quality.',
            r'We specialize in the field of automotive parts and are familiar with the adaptation needs of various vehicle models, especially focusing on providing customized solutions for your car. Thoughtful customer service is always available to answer your installation, adaptation, and other questions, allowing you to buy with confidence and use with ease.',
            r'offers the largest catalog of DIY videos for replacement parts, making installation easier and repairs more accessible. Our mission is to help you "View Before You Do," with hundreds of new videos added weekly.',
            r'YOUR TRUSTED DIY VIDEO SOURCE',
            r'TRUST & COMPATIBILITY',
            r'For over 25 years, has been the premium choice for direct-fit replacement parts. Confirm fitment with the Garage "confirmed fit" feature and review our full compatibility chart below before purchase.',
            r'Country Of Origin',
            r'United States',
            r'All parts supported bv a team of oroduct experts based ir the United States',
            r'Trustworthy value - backed by team of engineers and quality control experts in the United States',
            r'Reliable design - engineered in the USA and backed by long history of automotive aftermarket experience',
        ]

        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        # 🔥 修复：保留换行符，只清理多余空格
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()
        return text

    def clean_amazon_words(self, text: str) -> str:
        """清理Amazon相关词汇（amazon, Amazon, amazon's, Amazon's）"""
        if pd.isna(text):
            return ""

        text = str(text)
        # 清理 amazon/Amazon/amazon's/Amazon's，不区分大小写
        text = re.sub(r"\bamazon(?:\'s)?\b", '', text, flags=re.IGNORECASE)
        # 清理多余空格，但保留换行符
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()
        return text

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

        # 重新组合，保留换行符
        return '\n'.join(unique_desc_lines) if unique_desc_lines else ""

    def is_allowed_brand(self, brand: str) -> bool:
        """检查品牌是否在白名单中（不区分大小写）"""
        if not brand:
            return False
        # 清理并标准化
        clean = brand.strip()
        # 不区分大小写比较
        return clean.upper() in {b.upper() for b in self.ALLOWED_BRANDS}

    def process_item_specifics(self, text: str) -> tuple[str, str, str, str]:
        """
        处理item specifics列
        返回: (核心参数, Brand(白名单), Brand_Other(非白名单), 剩余item_specifics)
        """
        if pd.isna(text) or not str(text).strip():
            return "", "", "", ""

        text = str(text).strip()

        # 解析数据
        data = {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                data = parsed
            elif isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                data = parsed[0]
        except:
            for line in text.split('\n'):
                line = line.strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    data[key.strip()] = value.strip()

        # 要提取的8个字段
        core_fields = [
            'Manufacturer', 'UPC', 'Brand', "Part Number",
            'Item model number', 'Manufacturer Part Number', 'Model', 'Style',
        ]

        # 需要检查数字比例的字段
        digit_check_fields = {
            'Part Number', 'Item model number',
            'Manufacturer Part Number', 'Model'
        }

        def should_keep_value(value: str) -> bool:
            """
            检查值是否保留：数字和字母中，数字占比超过50%则保留
            只计算数字和字母，忽略其他字符
            """
            clean_value = str(value).replace('\u200e', '').replace('\u200f', '').strip()

            # 统计数字和字母
            digits = sum(1 for c in clean_value if c.isdigit())
            letters = sum(1 for c in clean_value if c.isalpha())
            total_alphanumeric = digits + letters

            # 如果没有字母数字，保留原值（避免除零）
            if total_alphanumeric == 0:
                return True

            # 数字占比超过50%保留
            return (digits / total_alphanumeric) > 0.5

        # 提取核心参数
        core_params = []
        for field in core_fields:
            if field in data and data[field]:
                raw_value = str(data[field])

                # 对指定字段进行数字比例检查
                if field in digit_check_fields:
                    if not should_keep_value(raw_value):
                        continue  # 跳过不满足条件的字段

                value = raw_value.replace('\u200e', '').replace('\u200f', '').strip()
                core_params.append(f"{field}: {value}")

        核心参数 = '\n'.join(core_params)

        # 处理 Brand（分级处理）
        raw_brand = ""
        if 'Brand' in data and data['Brand']:
            raw_brand = str(data['Brand']).replace('\u200e', '').replace('\u200f', '').strip()

        # 检查是否在白名单中
        brand_in_whitelist = self.is_allowed_brand(raw_brand)

        # Brand列：只在白名单中时保留
        Brand = raw_brand if brand_in_whitelist else ""
        # Brand_Other列：不在白名单时保留（分级处理）
        Brand_Other = "" if brand_in_whitelist else raw_brand

        # 剩余字段处理 - 同时删除核心字段和未通过数字检查的字段
        remaining = {}
        for key, value in data.items():
            # 跳过核心字段（不包括Brand）
            if key in ['Manufacturer', 'Brand Name', 'UPC', 'Item model number',
                       'Manufacturer Part Number', 'Model', 'Style', 'ASIN', 'Date First Available',
                       'Is Discontinued By Manufacturer','Best Sellers Rank','Customer Reviews']:
                continue

            # 对需要检查的字段进行数字比例验证
            if key in digit_check_fields:
                if not should_keep_value(value):
                    continue  # 不满足50%数字比例，删除该字段

            remaining[key] = value

        # Brand处理：如果在白名单中，保留在item_specifics；否则从item_specifics删除
        if 'Brand' in remaining and not brand_in_whitelist:
            del remaining['Brand']

        # 格式化剩余字段
        remaining_lines = []
        for key, value in remaining.items():
            clean_value = str(value).replace('\u200e', '').replace('\u200f', '').strip()
            remaining_lines.append(f"{key}: {clean_value}")

        item_specifics剩余 = '\n'.join(remaining_lines)

        return 核心参数, Brand, Brand_Other, item_specifics剩余

    def run(self) -> None:
        """主流程"""
        print(f"[1/6] 读取源文件: {self.src_file}")
        df = pd.read_excel(self.src_file)
        print(f"      成功读取 {len(df)} 行数据")
        print(f"      可用列: {list(df.columns)}")

        # 查找列名
        about_col = None
        possible_about_names = ['About_table', 'About', 'about_table', 'about']
        for name in possible_about_names:
            if name in df.columns:
                about_col = name
                break

        desc_col = 'description_from_the_seller' if 'description_from_the_seller' in df.columns else None

        # 1. 清理description_from_the_seller
        if desc_col is None:
            print(f"⚠️  警告: 未找到 'description_from_the_seller' 列")
        else:
            print("[2/6] 去除 Amazon 相关词汇...")
            df[desc_col] = df[desc_col].apply(self.clean_amazon_words)
            print("      Amazon词汇清理完成")

            print("[3/6] 清理 description_from_the_seller 列...")
            df[desc_col] = df[desc_col].apply(self.clean_description)
            print("      基础清理完成")


        # 2. 清理 About_table 列
        if about_col is None:
            print(f"⚠️  警告: 未找到 About_table 列")
        else:
            print(f"[4/6] 清理 '{about_col}' 列的 Amazon 词汇...")
            df[about_col] = df[about_col].apply(self.clean_amazon_words)
            print("      About_table Amazon词汇清理完成")

        # 3. 两列之间查重去重
        if about_col and desc_col:
            print(f"[5/6] 查重：删除 {desc_col} 中与 {about_col} 重复的内容...")
            df[desc_col] = df.apply(
                lambda row: self.remove_duplicates_between_columns(
                    row.get(about_col, ""),
                    row.get(desc_col, "")
                ),
                axis=1
            )
            print("      查重去重完成")
        else:
            print(f"⚠️  跳过查重：缺少必要的列")

        # 4. 查找item specifics列
        item_col = None
        possible_names = ['item_specifics', 'Item Specifics', 'item specifics', 'Item specifics', 'specifics']
        for name in possible_names:
            if name in df.columns:
                item_col = name
                break

        if item_col is None:
            print(f"⚠️  警告: 未找到 item specifics 列")
        else:
            print(f"[6/6] 处理 '{item_col}' 列...")
            results = df[item_col].apply(self.process_item_specifics)
            df['核心参数'] = results.apply(lambda x: x[0])
            df['Brand'] = results.apply(lambda x: x[1])
            df['Brand_Other'] = results.apply(lambda x: x[2])  # 新增：非白名单品牌
            df['item_specifics'] = results.apply(lambda x: x[3])

            if item_col != 'item_specifics':
                df = df.drop(columns=[item_col])

            print("      核心参数、Brand、Brand_Other、item_specifics 生成完成")

        # 保存
        print("[保存] 保存文件...")
        self.dst_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(self.dst_file, index=False)
        print(f"\n✅ 已保存到: {self.dst_file.resolve()}")


def main():
    cleaner = ExcelCleaner()
    cleaner.run()


if __name__ == "__main__":
    main()