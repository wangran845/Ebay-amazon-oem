from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from collections import defaultdict
import os


class ReviewProcessor:
    """处理评论数据，生成目标格式（融合版 - 支持直接读取review_detail表格）"""

    STAR_PATTERN = re.compile(r'(\d+)\.?\d*\s*out of\s*5\s*stars', re.IGNORECASE)

    def __init__(
            self,
            input_file: str,  # 输入文件：可以是review_detail.xlsx或包含seller_feedback的原始文件
            b_file: str = None,  # B.xlsx 包含 OEM 和 图片本地地址 列（可选）
            c_file: str = None,  # C.csv 包含 media_gallery 和 sku 列（可选）
            review_id_file: str = "review_id.txt",
            output_file: str | None = None,
            delete_low_rating: bool = True,
            status_id: int = 1,
            deduplicate: bool = True,
            deduplicate_fields: list = None,
            blacklist_file: str = None,
            blacklist_fields: list = None,
            replace_text: str = "okcarpart",
            # 新增：直接读取review_detail模式的标志
            direct_review_mode: bool = False,
            oem_mapping_file: str = None,  # OEM到SKU的映射文件（可选）
    ):
        # 清理路径
        self.input_file = self._clean_path(input_file)
        self.b_file = self._clean_path(b_file) if b_file else None
        self.c_file = self._clean_path(c_file) if c_file else None
        self.review_id_file = self._clean_path(review_id_file)
        self.direct_review_mode = direct_review_mode
        self.oem_mapping_file = self._clean_path(oem_mapping_file) if oem_mapping_file else None

        # 黑名单配置
        if blacklist_file:
            self.blacklist_file = self._clean_path(blacklist_file)
        else:
            self.blacklist_file = None

        if output_file is None:
            self.output_file = self.input_file.parent / f"{self.input_file.stem}_处理后.xlsx"
        else:
            self.output_file = Path(self._clean_path(output_file))

        self.delete_low_rating = delete_low_rating
        self.status_id = status_id
        self.deduplicate = deduplicate
        self.deduplicate_fields = deduplicate_fields or ['title', 'detail']
        self.blacklist_fields = blacklist_fields or ['title', 'detail']
        self.replace_text = replace_text
        self.blacklist_replace_count = 0
        self.blacklist = self._load_blacklist()

        self.start_id = self._load_start_id()
        self.current_id = self.start_id

        # 加载映射表（仅在非直接模式下）
        self.oem_to_img_path = {}
        self.img_to_sku = {}
        self.oem_to_sku_direct = {}  # 直接OEM到SKU的映射

        if not direct_review_mode:
            if b_file:
                self.oem_to_img_path = self._load_b_mapping()
            if c_file:
                self.img_to_sku = self._load_c_mapping()

        # 加载OEM直接映射（如果提供）
        if self.oem_mapping_file:
            self.oem_to_sku_direct = self._load_oem_sku_mapping()

    def _clean_path(self, path: str) -> Path:
        """清理路径字符串"""
        if not path:
            return Path("")
        path_str = str(path)
        cleaned = ''.join(char for char in path_str if ord(char) >= 32 or char in ['/', '\\', ':', '.', '-', '_', ' '])
        cleaned = re.sub(r'[\\/]+', '/', cleaned)
        if cleaned.startswith('/') and os.sep == '\\':
            cleaned = cleaned.replace('/', '\\', 1)
        return Path(cleaned)

    def _load_blacklist(self) -> list:
        """从Excel文件加载黑名单关键词"""
        if not self.blacklist_file:
            print("📋 未配置黑名单文件，跳过黑名单过滤")
            return []

        print(f"📂 加载黑名单文件: {self.blacklist_file}")

        if not self.blacklist_file.exists():
            print(f"   ⚠️ 找不到黑名单文件: {self.blacklist_file}")
            return []

        try:
            df = pd.read_excel(self.blacklist_file)
            keyword_col = None
            for col in df.columns:
                col_str = str(col).strip().lower()
                if '关键词' in col_str or 'keyword' in col_str or '黑名单' in col_str:
                    keyword_col = col
                    break

            if keyword_col is None:
                keyword_col = df.columns[0]

            blacklist = []
            for _, row in df.iterrows():
                keyword = str(row[keyword_col]).strip() if pd.notna(row[keyword_col]) else ""
                if keyword and keyword not in blacklist:
                    blacklist.append(keyword)

            print(f"   加载了 {len(blacklist)} 个黑名单关键词")
            return blacklist

        except Exception as e:
            print(f"   ❌ 读取黑名单文件失败: {e}")
            return []

    def _load_start_id(self) -> int:
        if self.review_id_file.exists():
            try:
                with open(self.review_id_file, 'r', encoding='utf-8') as f:
                    return int(f.read().strip())
            except (ValueError, IOError):
                pass
        return 1

    def _save_end_id(self):
        self.review_id_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.review_id_file, 'w', encoding='utf-8') as f:
            f.write(str(self.current_id))
        print(f"💾 review_id 已更新: {self.start_id} → {self.current_id}")

    def _normalize_path(self, path: str) -> str:
        """标准化路径用于匹配"""
        if not path:
            return ""
        path = path.strip().replace('\\', '/').lower()
        path = re.sub(r'^[a-z]:', '', path)
        return path

    def _get_filename(self, path: str) -> str:
        """获取文件名用于模糊匹配"""
        normalized = self._normalize_path(path)
        return normalized.split('/')[-1] if '/' in normalized else normalized

    def _find_alternative_paths(self, original_path: Path) -> list[Path]:
        """尝试查找替代路径"""
        alternatives = []
        current_dir = Path.cwd()

        if original_path.name:
            for file in current_dir.glob(f"**/{original_path.name}"):
                alternatives.append(file)

        clean_name = re.sub(r'[^\w\s.-]', '', original_path.name)
        if clean_name != original_path.name:
            for file in current_dir.glob(f"**/{clean_name}"):
                alternatives.append(file)

        return alternatives

    def _load_b_mapping(self) -> dict[str, str]:
        """加载 B.xlsx: OEM -> 图片本地地址"""
        if not self.b_file or not self.b_file.exists():
            return {}

        print(f"📂 加载 B.xlsx: {self.b_file}")
        df = pd.read_excel(self.b_file)

        oem_col = None
        img_col = None

        for col in df.columns:
            col_str = str(col).strip()
            if col_str.upper() == 'OEM':
                oem_col = col
            elif '图片本地地址' in col_str:
                img_col = col
            elif '本地' in col_str and ('路径' in col_str or '地址' in col_str or '图片' in col_str):
                img_col = col

        if oem_col is None:
            oem_col = df.columns[0]
        if img_col is None:
            for col in df.columns:
                sample = str(df[col].iloc[0]) if len(df) > 0 else ""
                if ':\\' in sample or sample.startswith(('E:', 'C:', 'D:', '/')):
                    img_col = col
                    break
            if img_col is None:
                img_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

        mapping = {}
        for _, row in df.iterrows():
            oem = str(row[oem_col]).strip() if pd.notna(row[oem_col]) else ""
            img_path = str(row[img_col]).strip() if pd.notna(row[img_col]) else ""
            if oem and img_path:
                mapping[oem] = img_path

        print(f"   加载了 {len(mapping)} 条 OEM->图片本地地址 映射")
        return mapping

    def _load_c_mapping(self) -> dict[str, str]:
        """加载 C.csv: media_gallery（图片本地地址） -> sku"""
        if not self.c_file or not self.c_file.exists():
            return {}

        print(f"📂 加载 C.csv: {self.c_file}")
        df = pd.read_csv(self.c_file)

        img_col = None
        sku_col = None

        for col in df.columns:
            col_str = str(col).strip().lower()
            if 'media' in col_str and 'gallery' in col_str:
                img_col = col
            elif col_str == 'sku':
                sku_col = col

        if img_col is None:
            for col in df.columns:
                sample = str(df[col].iloc[0]) if len(df) > 0 else ""
                if ':\\' in sample or sample.startswith(('E:', 'C:', 'D:', '/')) or '/' in sample:
                    img_col = col
                    break

        if sku_col is None:
            for col in df.columns:
                if str(col).strip().lower() in ['sku', 'skuid', 'sku_id']:
                    sku_col = col
                    break
            if sku_col is None:
                sku_col = df.columns[0]

        if img_col is None:
            print(f"   ⚠️ C.csv中未找到图片路径列")
            return {}

        mapping = {}
        for _, row in df.iterrows():
            img_path = str(row[img_col]).strip() if pd.notna(row[img_col]) else ""
            sku = str(row[sku_col]).strip() if pd.notna(row[sku_col]) else ""
            if img_path and sku:
                normalized_path = self._normalize_path(img_path)
                if normalized_path not in mapping:
                    mapping[normalized_path] = sku

        print(f"   加载了 {len(mapping)} 条 图片路径->sku 映射")
        return mapping

    def _load_oem_sku_mapping(self) -> dict[str, str]:
        """加载OEM到SKU的直接映射文件"""
        if not self.oem_mapping_file or not self.oem_mapping_file.exists():
            return {}

        print(f"📂 加载OEM-SKU映射: {self.oem_mapping_file}")

        try:
            if self.oem_mapping_file.suffix.lower() == '.csv':
                df = pd.read_csv(self.oem_mapping_file)
            else:
                df = pd.read_excel(self.oem_mapping_file)

            oem_col = None
            sku_col = None

            for col in df.columns:
                col_upper = str(col).strip().upper()
                if col_upper == 'OEM':
                    oem_col = col
                elif col_upper == 'SKU':
                    sku_col = col

            if oem_col is None:
                oem_col = df.columns[0]
            if sku_col is None:
                sku_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

            mapping = {}
            for _, row in df.iterrows():
                oem = str(row[oem_col]).strip() if pd.notna(row[oem_col]) else ""
                sku = str(row[sku_col]).strip() if pd.notna(row[sku_col]) else ""
                if oem and sku:
                    mapping[oem] = sku

            print(f"   加载了 {len(mapping)} 条 OEM->SKU 直接映射")
            return mapping

        except Exception as e:
            print(f"   ❌ 加载OEM-SKU映射失败: {e}")
            return {}

    def _get_sku(self, oem: str, existing_sku: str = "") -> tuple[str, str]:
        """通过OEM获取sku，返回(sku, 匹配方式)"""
        # 如果已经有SKU（直接读取模式），直接返回
        if existing_sku and existing_sku != "nan":
            return existing_sku, "已有SKU"

        # 优先使用直接OEM->SKU映射
        if oem in self.oem_to_sku_direct:
            return self.oem_to_sku_direct[oem], "OEM直接映射"

        # OEM -> 图片本地地址 -> SKU
        img_path = self.oem_to_img_path.get(oem, "")
        if not img_path:
            return "", "无OEM映射"

        normalized_path = self._normalize_path(img_path)

        # 方式1: 完整路径匹配
        sku = self.img_to_sku.get(normalized_path, "")
        if sku:
            return sku, "完整路径匹配"

        # 方式2: 文件名匹配
        filename = self._get_filename(img_path)
        for stored_path, stored_sku in self.img_to_sku.items():
            if stored_path.endswith('/' + filename) or stored_path == filename:
                return stored_sku, "文件名匹配"

        # 方式3: 部分路径匹配
        path_parts = normalized_path.split('/')
        for stored_path, stored_sku in self.img_to_sku.items():
            stored_parts = stored_path.split('/')
            if len(path_parts) >= 3 and len(stored_parts) >= 3:
                if path_parts[-3:] == stored_parts[-3:]:
                    return stored_sku, "部分路径匹配"

        return "", "未匹配"

    def _extract_rating(self, text: str) -> int | None:
        match = self.STAR_PATTERN.search(text)
        if match:
            return int(match.group(1))
        return None

    def _clean_feedback(self, text: str) -> str:
        """清洗 seller_feedback 内容"""
        if pd.isna(text) or not str(text).strip():
            return ""

        text = str(text)
        # 将 amazon 替换成 okcarpart
        text = re.sub(r'amazon', 'okcarpart', text, flags=re.IGNORECASE)

        lines = text.split('\n')
        cleaned_lines = []
        seen_non_rating_lines = set()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            is_rating_line = self.STAR_PATTERN.match(line)

            if is_rating_line:
                cleaned_lines.append(line)
            else:
                if re.match(r'^(Model|Size|Color)\s*:', line, re.IGNORECASE):
                    continue
                if line in seen_non_rating_lines:
                    continue
                seen_non_rating_lines.add(line)
                cleaned_lines.append(line)

        return '\n'.join(cleaned_lines)

    def _replace_blacklist_words(self, text: str) -> str:
        """替换文本中的黑名单关键词"""
        if not self.blacklist or not text:
            return text

        replaced_text = text
        for keyword in self.blacklist:
            pattern = re.compile(re.escape(keyword), re.IGNORECASE)
            replaced_text = pattern.sub(self.replace_text, replaced_text)

        return replaced_text

    def _process_blacklist(self, title: str, detail: str) -> tuple[str, str, bool]:
        """处理黑名单关键词（替换）"""
        if not self.blacklist:
            return title, detail, False

        replaced = False
        new_title = title
        new_detail = detail

        if 'title' in self.blacklist_fields:
            new_title = self._replace_blacklist_words(title)
            if new_title != title:
                replaced = True

        if 'detail' in self.blacklist_fields:
            new_detail = self._replace_blacklist_words(detail)
            if new_detail != detail:
                replaced = True

        if replaced:
            self.blacklist_replace_count += 1

        return new_title, new_detail, replaced

    def split_reviews(self, text: str, oem: str) -> list[dict]:
        """分割多条评论（用于原始seller_feedback格式）"""
        text = self._clean_feedback(text)

        if not text:
            return []

        star_matches = list(self.STAR_PATTERN.finditer(text))

        if not star_matches:
            parsed = self._parse_single_review(text, oem)
            return [parsed] if parsed else []

        reviews = []
        for i, match in enumerate(star_matches):
            rating = int(match.group(1))
            star_end = match.end()

            if i == 0:
                review_start = 0
            else:
                review_start = star_matches[i - 1].end()

            review_text = text[review_start:star_end].strip()
            parsed = self._parse_single_review(review_text, oem, rating)
            if parsed:
                reviews.append(parsed)

        return reviews

    def _parse_single_review(self, text: str, oem: str, rating: int | None = None) -> dict | None:
        """解析单条评论（用于原始seller_feedback格式）"""
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        if not lines:
            return None

        if rating is None:
            rating = self._extract_rating(text)

        if self.delete_low_rating and rating in [0, 1, 2, 3]:
            return None

        nickname = lines[0] if lines else "anonymous"
        if self.STAR_PATTERN.match(nickname):
            nickname = "anonymous"
            content_lines = lines[:]
        else:
            content_lines = lines[1:]

        filtered_lines = []
        for line in content_lines:
            if self.STAR_PATTERN.match(line):
                continue
            if re.match(r'^(Model|Size)\s*:', line, re.IGNORECASE):
                continue
            filtered_lines.append(line)

        detail = ' '.join(filtered_lines).strip()

        title = ""
        if detail:
            first_period = detail.find('。')
            if first_period == -1:
                first_period = detail.find('.')

            if first_period != -1:
                title = detail[:first_period + 1].strip()
            else:
                title = detail[:50] + '...' if len(detail) > 50 else detail

        title, detail, replaced = self._process_blacklist(title, detail)

        sku, match_type = self._get_sku(oem)

        return {
            'nickname': nickname,
            'title': title,
            'detail': detail,
            'rating': rating if rating is not None else 0,
            'sku': sku,
            'oem': oem,
            'img_path': self.oem_to_img_path.get(oem, ""),
            'match_type': match_type
        }

    def _generate_deduplication_key(self, review: dict) -> str:
        """生成去重键"""
        key_parts = []
        for field in self.deduplicate_fields:
            value = review.get(field, '')
            if isinstance(value, str):
                value = re.sub(r'\s+', ' ', value.strip()).lower()
            key_parts.append(str(value))
        return '|'.join(key_parts)

    def _process_direct_review_format(self, df: pd.DataFrame) -> list[dict]:
        """处理直接的review_detail格式（如你提供的表格）"""
        all_reviews = []

        for idx, row in df.iterrows():
            # 提取现有字段
            detail_id = row.get('detail_id', self.current_id)
            review_id = row.get('review_id', self.current_id)
            store_id = row.get('store_id', 0)
            title = str(row.get('title', '')) if pd.notna(row.get('title')) else ''
            detail = str(row.get('detail', '')) if pd.notna(row.get('detail')) else ''
            nickname = str(row.get('nickname', 'anonymous')) if pd.notna(row.get('nickname')) else 'anonymous'
            status_id = row.get('status_id', self.status_id)
            sku = str(row.get('sku', '')) if pd.notna(row.get('sku')) else ''
            product_id = row.get('product_id', '')
            rating = row.get('rating', 0)

            # 处理低分评论
            if self.delete_low_rating and rating in [0, 1, 2, 3]:
                continue

            # 处理黑名单
            title, detail, replaced = self._process_blacklist(title, detail)

            # 从SKU提取OEM（如果SKU格式为 carpart-XXXXXXX）
            oem = ''
            if sku and '-' in str(sku):
                oem = str(sku).split('-')[-1]

            # 获取SKU（如果有映射）
            final_sku, match_type = self._get_sku(oem, sku)
            if not final_sku:
                final_sku = sku
                match_type = "原始SKU"

            review_data = {
                'OEM': oem,
                'detail_id': self.current_id,
                'review_id': self.current_id,
                'store_id': store_id,
                'title': title,
                'detail': detail,
                'nickname': nickname,
                'status_id': status_id,
                'sku': final_sku,
                'product_id': product_id if pd.notna(product_id) else '',
                'rating': rating,
                'B文件图片路径': self.oem_to_img_path.get(oem, ""),
                '匹配方式': match_type
            }

            all_reviews.append(review_data)
            self.current_id += 1

        return all_reviews

    def process(self) -> None:
        print(f"📂 读取输入文件: {self.input_file}")

        if not self.input_file.exists():
            raise FileNotFoundError(f"找不到输入文件: {self.input_file}")

        df = pd.read_excel(self.input_file)
        print(f"   读取了 {len(df)} 行数据")

        # 判断输入格式并处理
        if 'seller_feedback' in df.columns:
            # 原始格式：包含seller_feedback需要分割
            print("📋 检测到原始seller_feedback格式，进行评论分割...")
            all_reviews = self._process_original_format(df)
        elif 'detail' in df.columns and 'title' in df.columns:
            # 直接review_detail格式
            print("📋 检测到标准review_detail格式，直接处理...")
            all_reviews = self._process_direct_review_format(df)
        else:
            raise ValueError(f"无法识别的输入格式，可用列: {list(df.columns)}")

        # 显示黑名单统计
        if self.blacklist:
            print(f"\n🔍 黑名单处理统计:")
            print(f"   黑名单关键词数量: {len(self.blacklist)}")
            print(f"   被替换的评论数: {self.blacklist_replace_count} 条")

        # 去重逻辑
        original_count = len(all_reviews)
        if self.deduplicate and all_reviews:
            print(f"\n🔄 开始去重处理...")

            deduped_reviews = {}
            duplicate_records = defaultdict(list)

            for review in all_reviews:
                dedup_key = self._generate_deduplication_key(review)

                if dedup_key in deduped_reviews:
                    duplicate_records[dedup_key].append(review['detail_id'])
                else:
                    deduped_reviews[dedup_key] = review

            all_reviews = list(deduped_reviews.values())
            duplicate_count = original_count - len(all_reviews)

            if duplicate_count > 0:
                print(f"   ✅ 去重完成: 原始 {original_count} 条 → 去重后 {len(all_reviews)} 条")
            else:
                print(f"   ✅ 未发现重复评论")

        self._save_end_id()

        # 标准化输出列
        columns_order = [
            'OEM',
            'detail_id',
            'review_id',
            'store_id',
            'title',
            'detail',
            'nickname',
            'status_id',
            'sku',
            'product_id',
            'rating',
            'B文件图片路径',
            '匹配方式'
        ]

        result_df = pd.DataFrame(all_reviews)

        # 确保所有列都存在
        for col in columns_order:
            if col not in result_df.columns:
                result_df[col] = ''

        result_df = result_df[columns_order]
        result_df.to_excel(self.output_file, index=False)

        # 统计
        matched = result_df[result_df['sku'] != ''].shape[0]
        total = len(result_df)

        print(f"\n✅ 处理完成!")
        print(f"   输入行数: {len(df)}")
        print(f"   输出评论数: {total}")
        print(f"   SKU匹配成功: {matched}/{total} ({matched / total * 100:.1f}%)")
        print(f"   ID范围: {self.start_id} ~ {self.current_id - 1}")
        print(f"   删除低分评论: {'是' if self.delete_low_rating else '否'}")
        print(f"   去重功能: {'开启' if self.deduplicate else '关闭'}")
        print(f"   黑名单处理: {'开启' if self.blacklist else '关闭'}")
        print(f"   输出文件: {self.output_file}")

        print(f"\n📋 前3条预览:")
        print(result_df.head(3).to_string())

    def _process_original_format(self, df: pd.DataFrame) -> list[dict]:
        """处理原始seller_feedback格式"""
        oem_col = 'OEM' if 'OEM' in df.columns else 'OME'
        if oem_col not in df.columns:
            raise ValueError(f"缺少 OEM/OME 列")

        all_reviews = []

        for idx, row in df.iterrows():
            oem = str(row[oem_col]).strip() if pd.notna(row[oem_col]) else ""
            feedback = row.get('seller_feedback', '')

            reviews = self.split_reviews(feedback, oem)

            for review in reviews:
                review_id = self.current_id
                self.current_id += 1

                all_reviews.append({
                    'OEM': review['oem'],
                    'detail_id': review_id,
                    'review_id': review_id,
                    'store_id': 0,
                    'title': review['title'],
                    'detail': review['detail'],
                    'nickname': review['nickname'],
                    'status_id': self.status_id,
                    'sku': review['sku'],
                    'product_id': '',
                    'rating': review['rating'],
                    'B文件图片路径': review['img_path'],
                    '匹配方式': review['match_type']
                })

        return all_reviews

    def run(self) -> None:
        try:
            self.process()
        except Exception as e:
            print(f"❌ 错误: {e}")
            raise


# ==================== 使用示例 ====================

def main():
    """主函数 - 处理review_detail(1).xlsx示例"""

    # 配置
    INPUT_FILE = r"cleaned_feedback_clean.xlsx"  # 修改为你的路径
    OUTPUT_FILE = r"review_detail_processed.xlsx"  # 输出文件
    REVIEW_ID_FILE = r"C:\Users\Administrator\Desktop\review_id.txt"  # ID记录文件

    # 可选：黑名单文件
    BLACKLIST_FILE = None  # 或 r"C:\Users\Administrator\Desktop\Brand_WaterPumps.xlsx"

    # 创建处理器（直接读取review_detail模式）
    processor = ReviewProcessor(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        review_id_file=REVIEW_ID_FILE,
        delete_low_rating=True,  # 删除1-3星评论
        status_id=1,
        deduplicate=True,  # 启用去重
        deduplicate_fields=['title', 'detail'],  # 按title和detail去重
        blacklist_file=BLACKLIST_FILE,
        blacklist_fields=['title', 'detail'],
        replace_text="okcarpart",
        direct_review_mode=True,  # 直接读取review_detail模式
    )

    processor.run()

    return processor


# 执行
if __name__ == "__main__":
    processor = main()