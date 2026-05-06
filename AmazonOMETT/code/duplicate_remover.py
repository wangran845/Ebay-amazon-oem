from __future__ import annotations


import pandas as pd
from pathlib import Path

from data.config import Config


class DuplicateRemover:
    """
    删除指定列的重复值，保留第一个
    """
    import os
    import pandas as pd
    from pathlib import Path
    from data.config import Config

    # 切换到项目根目录
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    print(f"当前工作目录: {os.getcwd()}")


    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        # 路径
        self.src_file =Path(fr"data/{self.product_user_2}/{self.product_user_2}_need_with_images.xlsx")
        self.dst_file = Path(f"data/{self.product_user_2}/{self.product_user_2}_标题去重.xlsx")

        # 默认查重列（支持逗号分隔多列，如 "Title,OEM"）
        self.target_columns = "Title"

    def _parse_columns(self, column_str: str) -> list[str]:
        """解析列名字符串，支持逗号分隔"""
        return [c.strip() for c in column_str.split(',') if c.strip()]

    def run(self) -> None:
        """主流程"""
        print(f"[DuplicateRemover] 读取: {self.src_file}")
        df = pd.read_excel(self.src_file)

        before_count = len(df)
        print(f"                   成功读取 {before_count} 行数据")

        # 解析目标列
        target_cols = self._parse_columns(self.target_columns)

        # 查找实际存在的列
        found_cols = []
        for col in target_cols:
            if col in df.columns:
                found_cols.append(col)
            else:
                # 尝试大小写变体
                for actual_col in df.columns:
                    if actual_col.lower() == col.lower():
                        found_cols.append(actual_col)
                        break

        if not found_cols:
            print(f"⚠️  警告: 未找到目标列 {target_cols}，可用列: {list(df.columns)}")
            return

        print(f"[DuplicateRemover] 查重列: {found_cols}")

        # 统计重复
        dup_count = df[found_cols].duplicated().sum()
        print(f"[DuplicateRemover] 重复值数量: {dup_count}")

        # 删除重复，保留第一个
        df_clean = df.drop_duplicates(subset=found_cols, keep='first')

        after_count = len(df_clean)
        removed = before_count - after_count
        print(f"[DuplicateRemover] 删除后行数: {after_count} (删除 {removed} 行)")

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