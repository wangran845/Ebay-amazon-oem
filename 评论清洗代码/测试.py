import pandas as pd
import re
import os
from pathlib import Path

# ============ 配置区域 ============

INPUT_FILE = 'cleaned_feedback_clean.xlsx'  # 输入的Amazon格式数据
REVIEW_ID_FILE = 'review_id.txt'  # review_id记录文件
OUTPUT_FILE = 'review_output.xlsx'  # 输出文件

# 映射文件配置
OEM_IMAGE_XLSX = 'A_C_Compressors_TY(3)_已格式化.xlsx'  # OEM->图片本地地址映射（含OEM列、图片本地地址列）
SKU_CSV_PATH = 'A_C_Compressors_TY(3).csv'  # SKU映射CSV：含sku列、media_gallery列


# ============ 新增：大小写不敏感查找列名函数 ============

def find_column_case_insensitive(df, target_name):
    """大小写不敏感地查找列名，返回实际列名"""
    target_lower = target_name.lower()
    for col in df.columns:
        if col.lower() == target_lower:
            return col
    return None


# ============ 清洗函数 ============

def clean_text(text, remove_read_more=False):
    """清洗文本：保留指定字符，其他全部替换为空格"""
    if pd.isna(text):
        return ''

    digits = '0123456789'
    lowercase = 'abcdefghijklmnopqrstuvwxyz'
    uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    special = '~!@#$%^&*()_+`-={}|[]\\;:\'",./<>? '
    whitespace = '\n\r\t'

    allowed = digits + lowercase + uppercase + special + whitespace
    escaped_allowed = ''.join(re.escape(c) for c in allowed)

    pattern = f'[^{escaped_allowed}]'
    cleaned = re.sub(pattern, ' ', str(text))

    lines = cleaned.split('\n')
    cleaned_lines = []

    for line in lines:
        line = re.sub(r' +', ' ', line).strip()
        if line:
            cleaned_lines.append(line)

    # 删除最后一行单独的 "Read more"
    if remove_read_more and cleaned_lines:
        last_line = cleaned_lines[-1].strip()
        if re.match(r'^Read more(\s+Report)?$', last_line, re.IGNORECASE):
            cleaned_lines = cleaned_lines[:-1]

    return '\n'.join(cleaned_lines).strip()


# ============ 解析函数 ============

def parse_rating(rating_str):
    """从 '5.0 out of 5 stars' 提取数字"""
    if pd.isna(rating_str):
        return ''
    match = re.search(r'(\d+\.?\d*)', str(rating_str))
    return int(float(match.group(1))) if match else ''


def parse_time_location(col_3_str):
    """从 'Reviewed in the United States on December 23, 2025' 解析 time 和 location"""
    if pd.isna(col_3_str):
        return '', ''

    text = clean_text(col_3_str, remove_read_more=False)
    text_flat = ' '.join(text.split())

    patterns = [
        r'Reviewed in (.+?) on ([A-Za-z]+ \d{1,2}, \d{4})',
        r'Reviewed in (.+?) on (\d{1,2} [A-Za-z]+ \d{4})',
        r'Reviewed in (.+?) on ([A-Za-z]+ \d{4})',
        r'Reviewed in (.+?) on (.+)$',
    ]

    for pattern in patterns:
        match = re.search(pattern, text_flat)
        if match:
            return match.group(2).strip(), match.group(1).strip()

    return '', ''


# ============ 映射构建函数（修复版） ============

def build_oem_to_imagepath_mapping(xlsx_path):
    """
    从xlsx文件构建 OEM号 -> 图片本地地址 的映射
    支持列名：OEM/oem（大小写不敏感）、图片本地地址/image_path
    """
    oem_to_imagepath = {}

    if not xlsx_path or not os.path.exists(xlsx_path):
        print(f"Warning: OEM-Image XLSX {xlsx_path} does not exist")
        return oem_to_imagepath

    try:
        df = pd.read_excel(xlsx_path)
        print(f"  Loaded OEM-Image mapping: {len(df)} rows")
        print(f"  Columns: {list(df.columns)}")

        # 大小写不敏感查找列名
        oem_col = find_column_case_insensitive(df, 'oem')
        image_col = find_column_case_insensitive(df, 'image_path')

        # 如果没找到image_path，尝试中文列名"图片本地地址"
        if not image_col:
            image_col = find_column_case_insensitive(df, '图片本地地址')

        # 检查必要列
        if not oem_col:
            print(f"Error: XLSX must contain 'OEM' column (case insensitive)")
            return oem_to_imagepath

        if not image_col:
            print(f"Error: XLSX must contain '图片本地地址' or 'image_path' column")
            return oem_to_imagepath

        print(f"  Using columns: '{oem_col}' -> '{image_col}'")

        # 构建映射
        for _, row in df.iterrows():
            oem = str(row[oem_col]).strip().upper()
            image_path = str(row[image_col]).strip()

            if oem and image_path and oem != 'NAN':
                # 一个OEM可能对应多个图片，用列表存储
                if oem not in oem_to_imagepath:
                    oem_to_imagepath[oem] = []
                oem_to_imagepath[oem].append(image_path)

        print(f"  Mapped {len(oem_to_imagepath)} unique OEMs")

        # 显示样本
        sample_items = list(oem_to_imagepath.items())[:3]
        for oem, paths in sample_items:
            print(f"    {oem} -> {paths[:2]}{'...' if len(paths) > 2 else ''}")

    except Exception as e:
        print(f"Error reading OEM-Image XLSX: {e}")

    return oem_to_imagepath


def build_imagepath_to_sku_mapping(csv_path):
    """
    从CSV文件构建 图片路径 -> SKU 的映射
    CSV格式：sku列，media_gallery列（图片路径）
    """
    imagepath_to_sku = {}

    if not csv_path or not os.path.exists(csv_path):
        print(f"Warning: SKU CSV {csv_path} does not exist")
        return imagepath_to_sku

    try:
        df = pd.read_csv(csv_path)
        print(f"  Loaded SKU mapping: {len(df)} rows")
        print(f"  Columns: {list(df.columns)}")

        # 大小写不敏感查找列名
        sku_col = find_column_case_insensitive(df, 'sku')
        gallery_col = find_column_case_insensitive(df, 'media_gallery')

        # 检查必要列
        if not sku_col:
            print(f"Error: CSV must contain 'sku' column")
            return imagepath_to_sku

        if not gallery_col:
            print(f"Error: CSV must contain 'media_gallery' column")
            return imagepath_to_sku

        print(f"  Using columns: '{sku_col}' -> '{gallery_col}'")

        # 构建映射：media_gallery -> sku
        for _, row in df.iterrows():
            media_gallery = str(row[gallery_col]).strip()
            sku = str(row[sku_col]).strip()

            if media_gallery and sku and media_gallery != 'nan':
                # 标准化路径（小写，统一分隔符）
                path_key = media_gallery.lower().replace('\\', '/')
                imagepath_to_sku[path_key] = sku

                # 同时存储原始形式
                imagepath_to_sku[media_gallery.strip().lower()] = sku

        print(f"  Built {len(imagepath_to_sku)} imagepath-to-sku mappings")

    except Exception as e:
        print(f"Error reading SKU CSV: {e}")

    return imagepath_to_sku


def get_sku_by_oem_v2(oem, oem_to_imagepath, imagepath_to_sku):
    """
    通过OEM号获取SKU（新版映射逻辑）：
    OEM -> 图片本地地址 -> media_gallery匹配 -> SKU
    """
    if not oem:
        return ''

    oem_key = str(oem).strip().upper()

    # 检查OEM是否有对应的图片路径
    if oem_key not in oem_to_imagepath:
        return ''

    image_paths = oem_to_imagepath[oem_key]
    if not image_paths:
        return ''

    # 遍历所有图片路径，尝试匹配SKU
    for img_path in image_paths:
        # 标准化路径
        path_variants = [
            img_path.lower().replace('\\', '/'),  # 小写+正斜杠
            img_path.lower().replace('/', '\\'),  # 小写+反斜杠
            img_path.strip().lower(),  # 纯小写
            os.path.basename(img_path).lower(),  # 仅文件名
        ]

        # 尝试直接匹配
        for variant in path_variants:
            if variant in imagepath_to_sku:
                return imagepath_to_sku[variant]

        # 尝试部分匹配（media_gallery包含图片路径或反之）
        for gallery_path, sku in imagepath_to_sku.items():
            # 互相包含检查
            if variant in gallery_path or gallery_path in variant:
                return sku

            # 文件名匹配
            img_filename = os.path.basename(variant)
            gallery_filename = os.path.basename(gallery_path)
            if img_filename == gallery_filename:
                return sku

    return ''


# ============ ID管理函数 ============

def read_review_id(review_id_file):
    """读取review_id起始值"""
    if os.path.exists(review_id_file):
        try:
            with open(review_id_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    return int(content)
        except (ValueError, IOError):
            pass
    return 1


def write_review_id(review_id_file, next_id):
    """写入下一个review_id"""
    try:
        with open(review_id_file, 'w', encoding='utf-8') as f:
            f.write(str(next_id))
        return True
    except IOError:
        return False


# ============ 核心处理函数（修复版） ============

def process_cleaned_to_review_v2(input_file, review_id_file, output_file,
                                 oem_image_xlsx=None, sku_csv_path=None):
    """
    主处理流程：包含 OEM->图片本地地址->media_gallery->SKU 的完整映射链
    """

    print("=" * 70)
    print("Amazon评论数据清洗与格式转换（完整映射链版）")
    print("=" * 70)

    # ========== 步骤0: 构建双重映射 ==========
    print("\n[0/6] Building OEM->ImagePath->MediaGallery->SKU mappings...")

    # OEM -> 图片本地地址
    oem_to_imagepath = build_oem_to_imagepath_mapping(oem_image_xlsx)

    # 图片本地地址/media_gallery -> SKU
    imagepath_to_sku = build_imagepath_to_sku_mapping(sku_csv_path)

    # ========== 步骤1: 读取review_id ==========
    review_id_start = read_review_id(review_id_file)
    print(f"\n[1/6] Starting review_id from: {review_id_start}")

    # ========== 步骤2: 读取输入数据 ==========
    print(f"\n[2/6] Reading input file: {input_file}")
    try:
        df_cleaned = pd.read_excel(input_file)
        total_rows = len(df_cleaned)
        print(f"      Loaded {total_rows} rows")
        print(f"      Columns: {list(df_cleaned.columns)}")
    except Exception as e:
        print(f"Error: Failed to read {input_file}: {e}")
        return None

    # ========== 步骤3: 预计算所有OEM的SKU ==========
    print(f"\n[3/6] Pre-computing SKU for OEMs via dual mapping...")

    # 查找OME列（大小写不敏感）
    ome_col = find_column_case_insensitive(df_cleaned, 'ome')
    if not ome_col:
        print("Error: Cannot find 'OME' column in input file")
        return None

    print(f"      Using OME column: '{ome_col}'")

    # 收集所有唯一的OEM号
    unique_oems = set()
    for idx, row in df_cleaned.iterrows():
        if idx == 0 and str(row.get(ome_col, '')).upper() == ome_col.upper():
            continue
        oem = str(row.get(ome_col, '')).strip().upper()
        if oem and oem != 'NAN' and oem != ome_col.upper():
            unique_oems.add(oem)

    print(f"      Found {len(unique_oems)} unique OEMs in data")

    # 显示部分OEM样本
    if unique_oems:
        sample_oems = list(unique_oems)[:5]
        print(f"      Sample OEMs: {sample_oems}")

    # 为每个OEM预计算SKU
    oem_to_sku_cache = {}
    matched_count = 0

    for oem in unique_oems:
        sku = get_sku_by_oem_v2(oem, oem_to_imagepath, imagepath_to_sku)
        oem_to_sku_cache[oem] = sku
        if sku:
            matched_count += 1

    print(f"      Matched {matched_count}/{len(unique_oems)} OEMs to SKU")

    # 显示匹配样本
    matched_samples = [(oem, sku) for oem, sku in oem_to_sku_cache.items() if sku][:5]
    if matched_samples:
        print(f"\n      Sample matches:")
        for oem, sku in matched_samples:
            img_paths = oem_to_imagepath.get(oem, [])[:1]
            print(f"        {oem} -> {img_paths} -> {sku}")

    # ========== 步骤4: 处理数据 ==========
    print(f"\n[4/6] Processing data mapping and cleaning...")

    reviews = []
    current_review_id = review_id_start
    read_more_removed_count = 0

    # 查找所有列名（大小写不敏感）
    col_0 = find_column_case_insensitive(df_cleaned, 'col_0')
    col_1 = find_column_case_insensitive(df_cleaned, 'col_1')
    col_2 = find_column_case_insensitive(df_cleaned, 'col_2')
    col_3 = find_column_case_insensitive(df_cleaned, 'col_3')
    col_5 = find_column_case_insensitive(df_cleaned, 'col_5')

    for idx, row in df_cleaned.iterrows():
        # 跳过表头行
        if idx == 0 and str(row.get(ome_col, '')).upper() == ome_col.upper():
            continue

        # 获取OEM并查找对应SKU
        oem = str(row.get(ome_col, '')).strip().upper()
        sku = oem_to_sku_cache.get(oem, '')

        # 获取图片路径（用于调试）
        image_paths = oem_to_imagepath.get(oem, [])
        first_image = image_paths[0] if image_paths else ''

        # 提取并清洗各字段
        nickname = clean_text(row.get(col_0, ''), remove_read_more=False) if col_0 else ''
        rating = parse_rating(row.get(col_1, '')) if col_1 else ''
        title = clean_text(row.get(col_2, ''), remove_read_more=False) if col_2 else ''
        time_str, location = parse_time_location(row.get(col_3, '')) if col_3 else ('', '')

        # detail列：启用删除最后一行"Read more"
        detail_raw = row.get(col_5, '') if col_5 else ''
        detail = clean_text(detail_raw, remove_read_more=True)
        if clean_text(detail_raw, remove_read_more=False) != detail:
            read_more_removed_count += 1

        review = {
            'detail_id': len(reviews) + 1,
            'review_id': current_review_id,
            'store_id': 0,
            'title': title,
            'detail': detail,
            'nickname': nickname,
            'status_id': 1,
            'sku': sku,  # 通过完整映射链获取
            'product_id': '',
            'rating': rating,
            'time': time_str,
            'location': location,
            'oem': oem,  # 保留用于调试
            'image_path': first_image  # 保留用于调试
        }

        reviews.append(review)
        current_review_id += 1

        # 进度显示
        if (len(reviews)) % 1000 == 0:
            print(f"      Processed {len(reviews)}/{total_rows} rows...")

    print(f"      Completed: {len(reviews)} reviews processed")
    print(f"      Removed 'Read more' from {read_more_removed_count} entries")

    # ========== 步骤5: 保存结果 ==========
    print(f"\n[5/6] Saving results...")

    df_review = pd.DataFrame(reviews)

    # 列顺序
    column_order = [
        'detail_id', 'review_id', 'store_id', 'title', 'detail',
        'nickname', 'status_id', 'sku', 'product_id', 'rating',
        'time', 'location', 'oem', 'image_path'
    ]
    df_review = df_review[column_order]

    try:
        df_review.to_excel(output_file, index=False)
        print(f"      Saved to: {output_file}")
    except Exception as e:
        print(f"Error: Failed to save {output_file}: {e}")
        return df_review

    # ========== 步骤6: 更新review_id ==========
    print(f"\n[6/6] Updating review_id file...")
    next_review_id = current_review_id
    if write_review_id(review_id_file, next_review_id):
        print(f"      Updated {review_id_file} with next ID: {next_review_id}")

    # ========== 统计输出 ==========
    print("\n" + "=" * 70)
    print("处理统计")
    print("=" * 70)
    print(f"输入行数: {total_rows}")
    print(f"输出行数: {len(reviews)}")
    print(f"Review ID范围: {review_id_start} ~ {current_review_id - 1}")
    print(f"删除Read more数量: {read_more_removed_count}")
    print(f"\n映射统计:")
    print(f"  - OEM-ImagePath映射数: {len(oem_to_imagepath)}")
    print(f"  - ImagePath-SKU映射数: {len(imagepath_to_sku)}")
    print(
        f"  - OEM-SKU匹配率: {matched_count}/{len(unique_oems)} ({100 * matched_count / max(len(unique_oems), 1):.1f}%)")

    # SKU填充统计
    sku_filled = (df_review['sku'] != '').sum()
    print(f"  - 最终SKU填充率: {sku_filled}/{len(reviews)} ({100 * sku_filled / max(len(reviews), 1):.1f}%)")

    return df_review


# ============ 主程序入口 ============

if __name__ == '__main__':

    # 创建review_id.txt（如果不存在）
    if not os.path.exists(REVIEW_ID_FILE):
        with open(REVIEW_ID_FILE, 'w', encoding='utf-8') as f:
            f.write('1')
        print(f"Created {REVIEW_ID_FILE} with initial value 1\n")

    # 执行处理
    df_result = process_cleaned_to_review_v2(
        input_file=INPUT_FILE,
        review_id_file=REVIEW_ID_FILE,
        output_file=OUTPUT_FILE,
        oem_image_xlsx=OEM_IMAGE_XLSX,
        sku_csv_path=SKU_CSV_PATH
    )

    # 显示预览
    if df_result is not None and len(df_result) > 0:
        print("\n" + "=" * 70)
        print("数据预览（前3行）")
        print("=" * 70)
        preview_cols = ['detail_id', 'review_id', 'oem', 'image_path', 'sku', 'nickname', 'rating']
        print(df_result[preview_cols].head(3).to_string())