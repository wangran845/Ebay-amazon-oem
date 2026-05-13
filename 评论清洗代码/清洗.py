from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from collections import defaultdict
import os


class ReviewProcessor:
    """处理评论数据，生成目标格式（本地图片路径匹配版）"""

    STAR_PATTERN = re.compile(r'(\d+)\.?\d*\s*out of\s*5\s*stars', re.IGNORECASE)

    def __init__(
            self,
            input_file: str,  # A.xlsx 包含 seller_feedback 和 OEM
            b_file: str,  # B.xlsx 包含 OEM 和 图片本地地址 列
            c_file: str,  # C.csv 包含 media_gallery 和 sku 列
            review_id_file: str = "review_id.txt",
            output_file: str | None = None,
            delete_low_rating: bool = True,
            status_id: int = 1,
            deduplicate: bool = True,  # 新增：是否启用去重
            deduplicate_fields: list = None,  # 新增：去重字段列表
            blacklist_file: str = None,  # 新增：黑名单文件路径
            blacklist_fields: list = None,  # 新增：黑名单检查字段
            replace_text: str = "okcarpart"  # 新增：替换成的文本
    ):
        # 清理路径中的特殊字符
        self.input_file = self._clean_path(input_file)
        self.b_file = self._clean_path(b_file)
        self.c_file = self._clean_path(c_file)
        self.review_id_file = self._clean_path(review_id_file)

        # 黑名单文件路径
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
        # 默认去重字段：title和detail的组合作为唯一标识
        self.deduplicate_fields = deduplicate_fields or ['title', 'detail']

        # 黑名单配置
        self.blacklist_fields = blacklist_fields or ['title', 'detail']  # 默认在title和detail中检查
        self.replace_text = replace_text  # 替换成的文本
        self.blacklist_replace_count = 0  # 统计黑名单替换次数
        self.blacklist = self._load_blacklist()  # 从Excel加载黑名单

        self.start_id = self._load_start_id()
        self.current_id = self.start_id

        # 加载映射表（本地路径匹配）
        self.oem_to_img_path = self._load_b_mapping()
        self.img_to_sku = self._load_c_mapping()

    def _clean_path(self, path: str) -> Path:
        """清理路径字符串，移除不可见字符和特殊控制字符"""
        if not path:
            return Path("")

        # 转换为字符串并移除所有控制字符（ASCII 0-31）
        path_str = str(path)
        cleaned = ''.join(char for char in path_str if ord(char) >= 32 or char in ['/', '\\', ':', '.', '-', '_', ' '])

        # 替换多个连续的反斜杠或正斜杠为正斜杠（避免正则表达式转义问题）
        cleaned = re.sub(r'[\\/]+', '/', cleaned)

        # 确保路径以正确的分隔符开头
        if cleaned.startswith('/') and os.sep == '\\':
            # Windows系统下，将开头的/替换为\
            cleaned = cleaned.replace('/', '\\', 1)

        return Path(cleaned)

    def _load_blacklist(self) -> list:
        """从Excel文件加载黑名单关键词"""
        if not self.blacklist_file:
            print("📋 未配置黑名单文件，跳过黑名单过滤")
            return []

        print(f"📂 加载黑名单文件: {self.blacklist_file}")

        if not self.blacklist_file.exists():
            # 尝试其他可能的路径
            possible_paths = self._find_alternative_paths(self.blacklist_file)
            if possible_paths:
                self.blacklist_file = possible_paths[0]
                print(f"   找到替代文件: {self.blacklist_file}")
            else:
                print(f"   ⚠️ 找不到黑名单文件: {self.blacklist_file}")
                print(f"   将跳过黑名单过滤")
                return []

        try:
            df = pd.read_excel(self.blacklist_file)

            # 查找关键词列
            keyword_col = None
            for col in df.columns:
                col_str = str(col).strip().lower()
                if '关键词' in col_str or 'keyword' in col_str or '黑名单' in col_str:
                    keyword_col = col
                    break

            if keyword_col is None:
                # 如果没有找到指定列，使用第一列
                keyword_col = df.columns[0]
                print(f"   ⚠️ 未找到关键词列，使用第1列: {keyword_col}")
            else:
                print(f"   ✓ 找到关键词列: {keyword_col}")

            # 提取关键词
            blacklist = []
            for _, row in df.iterrows():
                keyword = str(row[keyword_col]).strip() if pd.notna(row[keyword_col]) else ""
                if keyword and keyword not in blacklist:  # 去重
                    blacklist.append(keyword)

            print(f"   加载了 {len(blacklist)} 个黑名单关键词")

            # 显示前10个关键词作为预览
            if blacklist:
                preview = blacklist[:10]
                print(f"   预览（前10个）: {preview}")
                if len(blacklist) > 10:
                    print(f"   ... 还有 {len(blacklist) - 10} 个关键词")

            return blacklist

        except Exception as e:
            print(f"   ❌ 读取黑名单文件失败: {e}")
            print(f"   将跳过黑名单过滤")
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
        # 确保目录存在
        self.review_id_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.review_id_file, 'w', encoding='utf-8') as f:
            f.write(str(self.current_id))
        print(f"💾 review_id 已更新: {self.start_id} → {self.current_id}")

    def _normalize_path(self, path: str) -> str:
        """标准化路径用于匹配（统一斜杠、去除首尾空格、转小写）"""
        if not path:
            return ""
        # 统一使用正斜杠，去除首尾空格，转小写
        path = path.strip().replace('\\', '/').lower()
        # 去除可能的盘符（如 C:）统一比较
        path = re.sub(r'^[a-z]:', '', path)
        return path

    def _get_filename(self, path: str) -> str:
        """获取文件名用于模糊匹配"""
        normalized = self._normalize_path(path)
        # 返回最后一部分（文件名）
        return normalized.split('/')[-1] if '/' in normalized else normalized

    def _find_alternative_paths(self, original_path: Path) -> list[Path]:
        """尝试查找替代路径"""
        alternatives = []

        # 获取当前工作目录
        current_dir = Path.cwd()

        # 尝试在当前目录查找
        if original_path.name:
            for file in current_dir.glob(f"**/{original_path.name}"):
                alternatives.append(file)

        # 尝试去掉特殊字符后查找
        clean_name = re.sub(r'[^\w\s.-]', '', original_path.name)
        if clean_name != original_path.name:
            for file in current_dir.glob(f"**/{clean_name}"):
                alternatives.append(file)

        # 尝试查找最近的文件名
        if not alternatives:
            # 获取文件名（不带路径）
            filename = original_path.name
            # 在所有子目录中查找包含该文件名的文件
            for file in current_dir.rglob(f"*{filename}*"):
                if file.is_file():
                    alternatives.append(file)
                    break  # 只取第一个找到的

        return alternatives

    def _load_b_mapping(self) -> dict[str, str]:
        """加载 B.xlsx: OEM -> 图片本地地址"""
        print(f"📂 加载 B.xlsx: {self.b_file}")

        if not self.b_file.exists():
            # 尝试其他可能的路径
            possible_paths = self._find_alternative_paths(self.b_file)
            if possible_paths:
                self.b_file = possible_paths[0]
                print(f"   找到替代文件: {self.b_file}")
            else:
                raise FileNotFoundError(f"找不到文件: {self.b_file}")

        df = pd.read_excel(self.b_file)

        # 找列
        oem_col = None
        img_col = None

        for col in df.columns:
            col_str = str(col).strip()
            if col_str.upper() == 'OEM':
                oem_col = col
            # 匹配"图片本地地址"或包含"本地"、"路径"、"地址"的列
            elif '图片本地地址' in col_str:
                img_col = col
            elif '本地' in col_str and ('路径' in col_str or '地址' in col_str or '图片' in col_str):
                img_col = col

        if oem_col is None:
            oem_col = df.columns[0]
            print(f"   ⚠️ 未找到'OEM'列，使用第1列: {oem_col}")

        if img_col is None:
            # 尝试找包含路径特征的列（如 E:\ 或 /data/）
            for col in df.columns:
                sample = str(df[col].iloc[0]) if len(df) > 0 else ""
                if ':\\' in sample or sample.startswith(('E:', 'C:', 'D:', '/')):
                    img_col = col
                    break

            if img_col is None:
                img_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
                print(f"   ⚠️ 未找到'图片本地地址'列，使用: {img_col}")
            else:
                print(f"   ✓ 找到疑似图片路径列: {img_col}")
        else:
            print(f"   ✓ 找到'图片本地地址'列: {img_col}")

        mapping = {}
        for _, row in df.iterrows():
            oem = str(row[oem_col]).strip() if pd.notna(row[oem_col]) else ""
            img_path = str(row[img_col]).strip() if pd.notna(row[img_col]) else ""
            if oem and img_path:
                mapping[oem] = img_path  # 保留原始路径

        print(f"   加载了 {len(mapping)} 条 OEM->图片本地地址 映射")
        return mapping

    def _load_c_mapping(self) -> dict[str, str]:
        """加载 C.csv: media_gallery（图片本地地址） -> sku"""
        print(f"📂 加载 C.csv: {self.c_file}")

        if not self.c_file.exists():
            possible_paths = self._find_alternative_paths(self.c_file)
            if possible_paths:
                self.c_file = possible_paths[0]
                print(f"   找到替代文件: {self.c_file}")
            else:
                raise FileNotFoundError(f"找不到文件: {self.c_file}")

        df = pd.read_csv(self.c_file)

        # 找列
        img_col = None
        sku_col = None

        for col in df.columns:
            col_str = str(col).strip().lower()
            if 'media' in col_str and 'gallery' in col_str:
                img_col = col
            elif col_str == 'sku':
                sku_col = col

        # 如果没找到media_gallery，尝试找包含路径的列
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
            raise ValueError(f"C.csv中未找到图片路径列(media_gallery)，可用列: {list(df.columns)}")

        print(f"   ✓ 找到图片路径列: {img_col}")
        print(f"   ✓ 找到sku列: {sku_col}")

        mapping = {}
        duplicate_paths = []

        for _, row in df.iterrows():
            img_path = str(row[img_col]).strip() if pd.notna(row[img_col]) else ""
            sku = str(row[sku_col]).strip() if pd.notna(row[sku_col]) else ""

            if img_path and sku:
                normalized_path = self._normalize_path(img_path)

                # 检查重复
                if normalized_path in mapping:
                    duplicate_paths.append(normalized_path[:50])
                    continue

                mapping[normalized_path] = sku

        if duplicate_paths:
            print(f"   ⚠️ 发现 {len(duplicate_paths)} 个重复图片路径（已跳过）")

        print(f"   加载了 {len(mapping)} 条 图片路径->sku 映射")
        return mapping

    def _get_sku(self, oem: str) -> tuple[str, str]:
        """通过OEM获取sku（本地图片路径匹配），返回(sku, 匹配方式)"""
        # OEM -> 图片本地地址
        img_path = self.oem_to_img_path.get(oem, "")
        if not img_path:
            return "", "无OEM映射"

        normalized_path = self._normalize_path(img_path)

        # 方式1: 完整路径匹配
        sku = self.img_to_sku.get(normalized_path, "")
        if sku:
            return sku, "完整路径匹配"

        # 方式2: 文件名匹配（如果路径结构不同但文件名相同）
        filename = self._get_filename(img_path)
        for stored_path, stored_sku in self.img_to_sku.items():
            if stored_path.endswith('/' + filename) or stored_path == filename:
                return stored_sku, "文件名匹配"

        # 方式3: 部分路径匹配（如 E:/ome_picture/123/1.jpg 匹配 /data/ome_picture/123/1.jpg）
        path_parts = normalized_path.split('/')
        for stored_path, stored_sku in self.img_to_sku.items():
            stored_parts = stored_path.split('/')
            # 检查后3级目录+文件名是否一致
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
        """
        清洗 seller_feedback 内容：
        1. 将 amazon 替换成 okcarpart
        2. 删除重复行（不包括星级评分行）
        3. 删除 Model: 和 Size: 开头的行
        """
        if pd.isna(text) or not str(text).strip():
            return ""

        text = str(text)

        # 1. 将 amazon 替换成 okcarpart（不区分大小写）
        text = re.sub(r'amazon', 'okcarpart', text, flags=re.IGNORECASE)

        # 分割成行
        lines = text.split('\n')
        cleaned_lines = []
        seen_non_rating_lines = set()  # 用于检测重复行（不包括星级评分）

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 检查是否是星级评分行
            is_rating_line = self.STAR_PATTERN.match(line)

            if is_rating_line:
                # 星级评分行直接保留
                cleaned_lines.append(line)
            else:
                # 2. 删除 Model: 和 Size: 开头的行
                if re.match(r'^(Model|Size|Color)\s*:', line, re.IGNORECASE):
                    continue

                # 3. 删除重复的非评分行
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
            # 不区分大小写替换
            pattern = re.compile(re.escape(keyword), re.IGNORECASE)
            replaced_text = pattern.sub(self.replace_text, replaced_text)

        return replaced_text

    def _process_blacklist(self, title: str, detail: str) -> tuple[str, str, bool]:
        """
        处理黑名单关键词（替换）
        返回: (新title, 新detail, 是否进行了替换)
        """
        if not self.blacklist:
            return title, detail, False

        replaced = False
        new_title = title
        new_detail = detail

        # 处理标题
        if 'title' in self.blacklist_fields:
            new_title = self._replace_blacklist_words(title)
            if new_title != title:
                replaced = True

        # 处理详情
        if 'detail' in self.blacklist_fields:
            new_detail = self._replace_blacklist_words(detail)
            if new_detail != detail:
                replaced = True

        if replaced:
            self.blacklist_replace_count += 1

        return new_title, new_detail, replaced

    def split_reviews(self, text: str, oem: str) -> list[dict]:
        # 先清洗文本
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
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        if not lines:
            return None

        if rating is None:
            rating = self._extract_rating(text)

        if self.delete_low_rating and rating in [0, 1, 2, 3]:
            return None

        # 获取用户名：第一行，如果没有则用 anonymous
        nickname = lines[0] if lines else "anonymous"
        # 如果第一行是星级评分，则没有用户名
        if self.STAR_PATTERN.match(nickname):
            nickname = "anonymous"
            content_lines = lines[:]  # 所有行都是内容
        else:
            content_lines = lines[1:]  # 除第一行外都是内容

        # 过滤掉星级评分行和Model:/Size:行
        filtered_lines = []
        for line in content_lines:
            if self.STAR_PATTERN.match(line):
                continue
            if re.match(r'^(Model|Size)\s*:', line, re.IGNORECASE):
                continue
            filtered_lines.append(line)

        detail = ' '.join(filtered_lines).strip()

        # 标题：第一句话（遇到句号）
        title = ""
        if detail:
            first_period = detail.find('。')
            if first_period == -1:
                first_period = detail.find('.')

            if first_period != -1:
                title = detail[:first_period + 1].strip()
            else:
                title = detail[:50] + '...' if len(detail) > 50 else detail

        # 处理黑名单关键词（替换）
        title, detail, replaced = self._process_blacklist(title, detail)

        sku, match_type = self._get_sku(oem)

        return {
            'nickname': nickname,
            'title': title,
            'detail': detail,
            'rating': rating if rating is not None else 0,
            'sku': sku,
            'oem': oem,
            'img_path': self.oem_to_img_path.get(oem, ""),  # B文件的原始路径
            'match_type': match_type  # 匹配方式
        }

    def _generate_deduplication_key(self, review: dict) -> str:
        """生成去重键"""
        key_parts = []
        for field in self.deduplicate_fields:
            value = review.get(field, '')
            # 清理和规范化文本以提高匹配准确度
            if isinstance(value, str):
                # 移除多余空格和标点符号
                value = re.sub(r'\s+', ' ', value.strip())
                # 转为小写
                value = value.lower()
            key_parts.append(str(value))
        return '|'.join(key_parts)

    def process(self) -> None:
        print(f"📂 读取输入文件: {self.input_file}")

        if not self.input_file.exists():
            possible_paths = self._find_alternative_paths(self.input_file)
            if possible_paths:
                self.input_file = possible_paths[0]
                print(f"   找到替代文件: {self.input_file}")
            else:
                raise FileNotFoundError(f"找不到输入文件: {self.input_file}")

        df = pd.read_excel(self.input_file)

        if 'seller_feedback' not in df.columns:
            raise ValueError(f"缺少 seller_feedback 列，可用: {list(df.columns)}")

        oem_col = 'OEM' if 'OEM' in df.columns else 'OME'
        if oem_col not in df.columns:
            raise ValueError(f"缺少 OEM/OME 列，可用: {list(df.columns)}")

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
                    'B文件图片路径': review['img_path'],  # 调试用
                    '匹配方式': review['match_type']  # 调试用
                })

        # 显示黑名单统计
        if self.blacklist:
            print(f"\n🔍 黑名单处理统计:")
            print(f"   黑名单关键词数量: {len(self.blacklist)}")
            print(f"   检查字段: {self.blacklist_fields}")
            print(f"   替换文本: {self.replace_text}")
            print(f"   被替换的评论数: {self.blacklist_replace_count} 条")

        # ============ 去重逻辑 ============
        original_count = len(all_reviews)
        if self.deduplicate and all_reviews:
            print(f"\n🔄 开始去重处理...")
            print(f"   去重字段: {self.deduplicate_fields}")

            # 使用字典进行去重
            deduped_reviews = {}
            duplicate_records = defaultdict(list)

            for review in all_reviews:
                # 生成去重键
                dedup_key = self._generate_deduplication_key(review)

                if dedup_key in deduped_reviews:
                    # 记录重复信息（用于统计）
                    duplicate_records[dedup_key].append(review['detail_id'])
                else:
                    deduped_reviews[dedup_key] = review

            all_reviews = list(deduped_reviews.values())

            # 统计去重结果
            duplicate_count = original_count - len(all_reviews)
            if duplicate_count > 0:
                print(f"   ✅ 去重完成:")
                print(f"      原始评论数: {original_count}")
                print(f"      去重后评论数: {len(all_reviews)}")
                print(f"      删除重复评论: {duplicate_count} 条")

                # 显示重复最多的前几个
                if duplicate_records:
                    print(f"      重复最多的记录:")
                    sorted_dupes = sorted(duplicate_records.items(),
                                          key=lambda x: len(x[1]),
                                          reverse=True)[:3]
                    for key, ids in sorted_dupes:
                        print(f"        - 键: {key[:50]}... 重复次数: {len(ids) + 1}")
            else:
                print(f"   ✅ 未发现重复评论")
        # ============ 去重逻辑结束 ============

        self._save_end_id()

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
        result_df = result_df[columns_order]

        result_df.to_excel(self.output_file, index=False)

        # 统计
        matched = result_df[result_df['sku'] != ''].shape[0]
        total = len(result_df)
        match_types = result_df['匹配方式'].value_counts().to_dict()

        print(f"\n✅ 处理完成!")
        print(f"   输入行数: {len(df)}")
        print(f"   输出评论数: {total}")
        print(f"   SKU匹配成功: {matched}/{total} ({matched / total * 100:.1f}%)")
        print(f"   匹配方式统计:")
        for mtype, count in match_types.items():
            print(f"      - {mtype}: {count}")
        print(f"   ID范围: {self.start_id} ~ {self.current_id - 1}")
        print(f"   删除低分评论: {'是' if self.delete_low_rating else '否'}")
        print(f"   去重功能: {'开启' if self.deduplicate else '关闭'}")
        print(f"   黑名单处理: {'开启' if self.blacklist else '关闭'}")
        if self.blacklist:
            print(f"   黑名单替换次数: {self.blacklist_replace_count} 条评论")
        print(f"   输出文件: {self.output_file}")

        # 显示未匹配的
        unmatched = result_df[result_df['sku'] == '']['OEM'].unique()
        if len(unmatched) > 0:
            print(f"\n⚠️ 未匹配到SKU的OEM（前10个）: {list(unmatched[:10])}")

        print(f"\n📋 前3条预览:")
        print(result_df.head(3).to_string())

        print(f"\n💡 提示: 'B文件图片路径'和'匹配方式'列用于验证，确认无误后可手动删除")

    def run(self) -> None:
        try:
            self.process()
        except Exception as e:
            print(f"❌ 错误: {e}")
            raise


def main():
    # ==================== 配置区域 ====================

    # 文件路径 - 使用原始字符串避免转义问题
    INPUT_FILE = r"C:\Users\19505\Desktop\ABS_relay_valve_XL(10)_need_with_images.xlsx"  # 包含 seller_feedback 和 OEM 列
    B_FILE = r"C:\Users\19505\Desktop\ABS_relay_valve_XL(10)_已格式化.xlsx"  # 包含 OEM 和 标题 列
    C_FILE = r"C:\Users\19505\Desktop\ABS_relay_valve_XL(10)_已格式化.csv"  # 包含 name 和 sku 列
    REVIEW_ID_FILE = r"C:\Users\19505\Desktop\review_id.txt"  # ID起始值存储文件

    # 输出文件（None则自动生成）
    OUTPUT_FILE = r"C:\Users\19505\Desktop\review_detail2.xlsx"  # 或指定 "data/输出.xlsx"

    # 是否删除 1,2,3 星评论
    DELETE_LOW_RATING = True  # False 则保留所有星级

    # status_id 固定值
    STATUS_ID = 1

    # 去重配置
    ENABLE_DEDUPLICATE = True  # 是否启用去重
    DEDUPLICATE_FIELDS = ['title', 'detail']  # 默认使用title和detail组合去重

    # ============ 黑名单配置 ============
    # 黑名单Excel文件路径（Excel文件中应包含关键词列）
    BLACKLIST_FILE = r"C:\Users\19505\Desktop\Brand_WaterPumps.xlsx"  # 黑名单文件路径

    # 黑名单检查的字段（默认在title和detail中检查）
    BLACKLIST_FIELDS = ['title', 'detail']  # 可选: ['title'], ['detail'], ['title', 'detail']

    # 黑名单替换成的文本
    REPLACE_TEXT = "okcarpart"  # 将黑名单关键词替换为这个文本

    # 是否启用黑名单过滤（如果文件不存在，会自动跳过）
    ENABLE_BLACKLIST = True  # 设为False则关闭黑名单过滤

    if not ENABLE_BLACKLIST:
        BLACKLIST_FILE = None
    # ====================================

    processor = ReviewProcessor(
        input_file=INPUT_FILE,
        b_file=B_FILE,
        c_file=C_FILE,
        review_id_file=REVIEW_ID_FILE,
        output_file=OUTPUT_FILE,
        delete_low_rating=DELETE_LOW_RATING,
        status_id=STATUS_ID,
        deduplicate=ENABLE_DEDUPLICATE,  # 传递去重参数
        deduplicate_fields=DEDUPLICATE_FIELDS,  # 传递去重字段
        blacklist_file=BLACKLIST_FILE,  # 传递黑名单文件路径
        blacklist_fields=BLACKLIST_FIELDS,  # 传递黑名单检查字段
        replace_text=REPLACE_TEXT  # 传递替换文本
    )
    processor.run()


if __name__ == "__main__":
    main()