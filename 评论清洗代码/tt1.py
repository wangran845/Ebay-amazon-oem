import pandas as pd
import re


def clean_seller_feedback(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗 seller_feedback 列
    """
    if 'seller_feedback' not in df.columns or 'OME' not in df.columns:
        return pd.DataFrame()

    records = []

    for idx, row in df.iterrows():
        ome = row['OME']
        feedback = str(row['seller_feedback'])

        # 分割
        parts = feedback.split('Report-%-')

        for part in parts[1:]:  # 跳过第一个
            part = part.strip()

            # 过滤条件：包含 <br>Report 的是垃圾行
            if '<br>Report' in part[:100]:
                continue

            # 过滤条件：以 Report<br> 开头的是垃圾行
            if part.startswith('Report<br>'):
                continue

            # 清理HTML
            part = re.sub(r'<[^>]+>', '', part)
            part = part.replace('&amp;', '&')
            part = part.strip()

            if not part or len(part) < 20:  # 太短的是垃圾
                continue

            # 分割列
            cols = part.split('-%-')

            # 清理：将 /?|\ 替换为换行（用普通字符串）
            cleaned_cols = []
            for c in cols:
                c = c.replace('/?|\\', '\n')  # 普通字符串，双反斜杠
                c = c.strip()
                if c:
                    cleaned_cols.append(c)

            if len(cleaned_cols) >= 3:
                record = {'OME': ome}
                for i, c in enumerate(cleaned_cols):
                    record[f'col_{i}'] = c
                records.append(record)

    return pd.DataFrame(records)


def fix_column_alignment(df: pd.DataFrame) -> pd.DataFrame:
    """
    统一列对齐（按顺序执行）：
    1. 检查col_1是否是评分格式(如"5.0 out of 5 stars")，如果不是则删除col_0并前移
    2. 检查col_3是否以"Reviewed in"开头，如果不是则删除并前移
    3. 检查col_4是否包含":"，如果存在则删除并前移
    4. 检查col_5是否只有"("半个括号，如果是则删除col_5和col_6并前移
    5. 最后：删除最后一列单独存在的"Read more"
    """
    if df.empty:
        return df

    # 评分格式正则
    rating_pattern = re.compile(r'^\d+\.\d+\s+out\s+of\s+\d+\s+stars?$', re.IGNORECASE)

    fixed_records = []

    for _, row in df.iterrows():
        # 获取当前行的所有列数据（除了OME）
        cols_data = {}
        for col in df.columns:
            if col.startswith('col_'):
                idx = int(col.split('_')[1])
                cols_data[idx] = row[col]

        # 步骤1：检查col_1是否是评分格式
        col_1_val = str(cols_data.get(1, '')).strip()
        if not rating_pattern.match(col_1_val):
            # col_1不是评分格式，删除col_0并前移
            new_cols = {}
            new_idx = 0
            for old_idx in sorted(cols_data.keys()):
                if old_idx == 0:
                    continue  # 跳过col_0
                new_cols[new_idx] = cols_data[old_idx]
                new_idx += 1
            cols_data = new_cols

        # 步骤2：检查col_3是否以"Reviewed in"开头
        col_3_val = str(cols_data.get(3, '')).strip()
        if not col_3_val.startswith('Reviewed in'):
            # 删除col_3并前移
            new_cols = {}
            new_idx = 0
            for old_idx in sorted(cols_data.keys()):
                if old_idx == 3:
                    continue  # 跳过col_3
                new_cols[new_idx] = cols_data[old_idx]
                new_idx += 1
            cols_data = new_cols

        # 步骤3：检查col_4是否包含":"
        col_4_val = str(cols_data.get(4, '')).strip()
        if ':' in col_4_val:
            # 删除col_4并前移
            new_cols = {}
            new_idx = 0
            for old_idx in sorted(cols_data.keys()):
                if old_idx == 4:
                    continue  # 跳过col_4
                new_cols[new_idx] = cols_data[old_idx]
                new_idx += 1
            cols_data = new_cols

        # 步骤4：检查col_5是否只有"("
        col_5_val = str(cols_data.get(5, '')).strip()
        if col_5_val == '(':
            # 删除col_5和col_6并前移
            new_cols = {}
            new_idx = 0
            for old_idx in sorted(cols_data.keys()):
                if old_idx in [5, 6]:
                    continue  # 跳过col_5和col_6
                new_cols[new_idx] = cols_data[old_idx]
                new_idx += 1
            cols_data = new_cols

        # 步骤5（最后）：删除最后一列单独存在的"Read more"
        # 步骤5（最后）：删除col_5中的"Read more"
        col_5_val = str(cols_data.get(5, '')).strip()
        if col_5_val == 'Read more':
            # 删除col_5并前移
            new_cols = {}
            new_idx = 0
            for old_idx in sorted(cols_data.keys()):
                if old_idx == 5:
                    continue  # 跳过col_5
                new_cols[new_idx] = cols_data[old_idx]
                new_idx += 1
            cols_data = new_cols



        # 构建新记录
        record = {'OME': row['OME']}
        for idx, val in cols_data.items():
            record[f'col_{idx}'] = val
        fixed_records.append(record)

    return pd.DataFrame(fixed_records)


if __name__ == '__main__':
    input_file = r'A_C_Compressors_TY(3)_need_with_images.xlsx'
    output_file = r'cleaned_feedback_clean.xlsx'

    df = pd.read_excel(input_file)
    cleaned_df = clean_seller_feedback(df)

    # 添加列对齐修复
    if not cleaned_df.empty:
        cleaned_df = fix_column_alignment(cleaned_df)

    if not cleaned_df.empty:
        cleaned_df.to_excel(output_file, index=False)
        print(f"提取 {len(cleaned_df)} 条评论")
    else:
        print("未提取到数据")