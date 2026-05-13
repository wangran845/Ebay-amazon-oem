import pandas as pd
import re
import os

# ============ 配置区域 ============

INPUT_FILE = 'cleaned_feedback_clean.xlsx'
REVIEW_ID_FILE = 'review_id.txt'
OUTPUT_FILE = 'review_output.xlsx'
SKU_CSV_PATH = None


# ============ 清洗函数 ============

def clean_text(text, remove_read_more=False):
    """
    清洗文本：保留指定字符，其他全部替换为空格

    保留字符：
    - 数字: 0-9
    - 小写字母: a到z
    - 大写字母: A到Z
    - 特殊符号: ~!@#$%^&*()_+`-={}|[]\;':",./<>?
    - 空格和换行符（\n \r \t）

    参数:
        remove_read_more: 是否删除最后一行单独的"Read more"
    """
    if pd.isna(text):
        return ''

    # 构建允许的字符集
    digits = '0123456789'
    lowercase = 'abcdefghijklmnopqrstuvwxyz'
    uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    special = '~!@#$%^&*()_+`-={}|[]\\;:\'",./<>? '
    whitespace = '\n\r\t'

    allowed = digits + lowercase + uppercase + special + whitespace

    # 逐个转义每个字符
    escaped_allowed = ''.join(re.escape(c) for c in allowed)

    # 匹配所有不允许的字符，替换为空格
    pattern = f'[^{escaped_allowed}]'
    cleaned = re.sub(pattern, ' ', str(text))

    # 分行处理，保留换行结构
    lines = cleaned.split('\n')
    cleaned_lines = []

    for line in lines:
        # 每行内部压缩连续空格
        line = re.sub(r' +', ' ', line).strip()
        cleaned_lines.append(line)

    # 删除空行
    cleaned_lines = [line for line in cleaned_lines if line]

    # 删除最后一行单独的 "Read more"（如果启用）
    if remove_read_more and cleaned_lines:
        last_line = cleaned_lines[-1].strip()
        # 匹配 "Read more" 或 "Read more Report" 等变体
        if re.match(r'^Read more(\s+Report)?$', last_line, re.IGNORECASE):
            cleaned_lines = cleaned_lines[:-1]

    # 重新组合
    cleaned = '\n'.join(cleaned_lines)

    return cleaned.strip()


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


def get_sku_list(csv_path):
    """从CSV文件读取sku列"""
    if not csv_path or not os.path.exists(csv_path):
        return None

    try:
        df = pd.read_csv(csv_path)
        if 'sku' in df.columns:
            return [clean_text(s, remove_read_more=False) for s in df['sku'].tolist()]
    except Exception as e:
        print(f"Error reading SKU CSV: {e}")

    return None


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


# ============ 核心处理函数 ============

def process_cleaned_to_review(input_file, review_id_file, output_file, sku_csv_path=None):
    """主处理流程"""

    print("=" * 60)
    print("Amazon评论数据清洗与格式转换")
    print("=" * 60)

    # 步骤1: 读取review_id
    review_id_start = read_review_id(review_id_file)
    print(f"\n[1/5] Starting review_id from: {review_id_start}")

    # 步骤2: 读取输入数据
    print(f"\n[2/5] Reading input file: {input_file}")
    try:
        df_cleaned = pd.read_excel(input_file)
        total_rows = len(df_cleaned)
        print(f"      Loaded {total_rows} rows")
    except Exception as e:
        print(f"Error: Failed to read {input_file}: {e}")
        return None

    # 步骤3: 获取SKU列表
    sku_list = get_sku_list(sku_csv_path)
    if sku_list:
        print(f"\n[3/5] Loaded {len(sku_list)} SKUs from CSV")
    else:
        print(f"\n[3/5] No SKU CSV provided")

    # 步骤4: 处理数据
    print(f"\n[4/5] Processing data mapping and cleaning...")

    reviews = []
    current_review_id = review_id_start
    read_more_removed_count = 0  # 统计删除的Read more数量

    for idx, row in df_cleaned.iterrows():
        # 跳过表头行
        if idx == 0 and str(row.get('OME', '')) == 'OME':
            continue

        # 提取并清洗各字段
        nickname = clean_text(row.get('col_0', ''), remove_read_more=False)
        rating = parse_rating(row.get('col_1', ''))
        title = clean_text(row.get('col_2', ''), remove_read_more=False)
        time_str, location = parse_time_location(row.get('col_3', ''))

        # detail列：启用删除最后一行"Read more"
        detail_raw = row.get('col_5', '')
        detail_before = clean_text(detail_raw, remove_read_more=False)
        detail_after = clean_text(detail_raw, remove_read_more=True)

        if detail_before != detail_after:
            read_more_removed_count += 1

        detail = detail_after

        # SKU
        sku = ''
        if sku_list and idx < len(sku_list):
            sku = sku_list[idx]

        review = {
            'detail_id': len(reviews) + 1,
            'review_id': current_review_id,
            'store_id': 0,
            'title': title,
            'detail': detail,
            'nickname': nickname,
            'status_id': 1,
            'sku': sku,
            'product_id': '',
            'rating': rating,
            'time': time_str,
            'location': location
        }

        reviews.append(review)
        current_review_id += 1

        # 进度显示
        if (len(reviews)) % 1000 == 0:
            print(f"      Processed {len(reviews)}/{total_rows} rows...")

    print(f"      Completed: {len(reviews)} reviews processed")
    print(f"      Removed 'Read more' from {read_more_removed_count} detail entries")

    # 步骤5: 保存结果
    print(f"\n[5/5] Saving results...")

    df_review = pd.DataFrame(reviews)

    column_order = [
        'detail_id', 'review_id', 'store_id', 'title', 'detail',
        'nickname', 'status_id', 'sku', 'product_id', 'rating',
        'time', 'location'
    ]
    df_review = df_review[column_order]

    try:
        df_review.to_excel(output_file, index=False)
        print(f"      Saved to: {output_file}")
    except Exception as e:
        print(f"Error: Failed to save {output_file}: {e}")
        return df_review

    # 更新review_id文件
    next_review_id = current_review_id
    if write_review_id(review_id_file, next_review_id):
        print(f"      Updated {review_id_file} with next ID: {next_review_id}")

    # 统计
    print("\n" + "=" * 60)
    print("处理统计")
    print("=" * 60)
    print(f"输入行数: {total_rows}")
    print(f"输出行数: {len(reviews)}")
    print(f"Review ID范围: {review_id_start} ~ {current_review_id - 1}")
    print(f"删除Read more数量: {read_more_removed_count}")

    return df_review


# ============ 主程序入口 ============

if __name__ == '__main__':

    # 创建review_id.txt（如果不存在）
    if not os.path.exists(REVIEW_ID_FILE):
        with open(REVIEW_ID_FILE, 'w', encoding='utf-8') as f:
            f.write('1')
        print(f"Created {REVIEW_ID_FILE} with initial value 1\n")

    # 执行处理
    df_result = process_cleaned_to_review(
        input_file=INPUT_FILE,
        review_id_file=REVIEW_ID_FILE,
        output_file=OUTPUT_FILE,
        sku_csv_path=SKU_CSV_PATH
    )

    # 显示预览
    if df_result is not None and len(df_result) > 0:
        print("\n" + "=" * 60)
        print("数据预览（前3行）")
        print("=" * 60)
        preview_cols = ['detail_id', 'review_id', 'nickname', 'rating', 'time', 'location', 'title']
        print(df_result[preview_cols].head(3).to_string())
        print(f"\ndetail列示例（第一行，前200字符）:")
        if len(df_result) > 0:
            sample_detail = df_result.iloc[0]['detail']
            print(repr(sample_detail[:200]) if len(str(sample_detail)) > 200 else repr(sample_detail))