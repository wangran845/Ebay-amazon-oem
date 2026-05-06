from __future__ import annotations

import pandas as pd
import re
from pathlib import Path
from data.config import Config
from bs4 import BeautifulSoup


class OEMUsageFilter:
    """筛选未使用的VBS编号或OEM编号（嵌在主程序最后执行）"""

    OEM_SEPARATORS = re.compile(r'[\\/;,|\n\r]+')
    HTML_COLUMNS = ['产品描述', '短描述']

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.product_name = self.config.product_user_2

        self.folder = Path(f"data/{self.product_name}")
        self.file_a = self.folder / f"{self.product_name}.xlsx"
        self.file_b = self.folder / f"{self.product_name}_已格式化.xlsx"

        # 列名配置（动态检测）
        self.vbs_col = None  # 动态检测：VBS No. / VBSNO / VBS
        self.oem_col_a = "OEM NO"
        self.oem_col_b = "OEM"

        # 模式标记
        self.mode = "vbs"  # "vbs" 或 "oem"

        self.stats = {
            'total_vbs': 0,
            'used_vbs': 0,
            'unused_vbs': 0,
            'total_a_rows': 0,
            'b_oem_count': 0,
            'output_rows': 0,
            'html_cleaned': 0,
            'mode': ''  # 记录实际使用的筛选模式
        }

    @staticmethod
    def html_to_text(html_content: str) -> str:
        """将HTML内容转换为可阅读纯文本"""
        if pd.isna(html_content) or not str(html_content).strip():
            return ''

        html_str = str(html_content)

        # 预替换块级标签为换行符
        html_str = re.sub(r'<\s*tr\s*>', '\n', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*/\s*tr\s*>', '', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*td\s*[^>]*>', ' ', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*/\s*td\s*>', ' ', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*th\s*[^>]*>', ' ', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*/\s*th\s*>', ' ', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*(?:p|h[1-6])\s*[^>]*>(.*?)</\s*(?:p|h[1-6])\s*>', r'\1\n\n',
                          html_str, flags=re.IGNORECASE | re.DOTALL)
        html_str = re.sub(r'<\s*li\s*>', '• ', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*/\s*li\s*>', '\n', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'<\s*div\s*[^>]*>(.*?)</\s*div\s*>', r'\1\n',
                          html_str, flags=re.IGNORECASE | re.DOTALL)
        html_str = re.sub(r'<\s*br\s*/?\s*>', '\n', html_str, flags=re.IGNORECASE)

        try:
            soup = BeautifulSoup(html_str, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'[\s\xa0]+', ' ', text)
            text = re.sub(r'\n\s*\n+', '\n\n', text)
            return text.strip()
        except Exception:
            text = re.sub(r'<[^>]+>', '', html_str)
            return re.sub(r'\s+', ' ', text).strip()

    def clean_html_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理DataFrame中的HTML列"""
        df_clean = df.copy()
        cleaned_count = 0

        for col in self.HTML_COLUMNS:
            if col in df_clean.columns:
                print(f"      🧹 清理HTML列: {col}")
                df_clean[col] = df_clean[col].apply(self.html_to_text)
                original_non_null = df[col].notna().sum()
                cleaned_count += original_non_null

        self.stats['html_cleaned'] = cleaned_count
        return df_clean

    def _detect_vbs_column(self, columns: list[str]) -> str | None:
        """动态检测VBS列名（支持 VBS No. / VBSNO / VBS）"""
        columns_lower = [c.strip().lower() for c in columns]

        # 优先级顺序
        candidates = ['WBS No.','WBS No.','vbs no.', 'vbsno', 'vbs no', 'vbs']

        for candidate in candidates:
            for i, col_lower in enumerate(columns_lower):
                if candidate == col_lower or candidate.replace(' ', '') == col_lower.replace(' ', ''):
                    return columns[i]  # 返回原始列名

        return None

    def check_files(self) -> bool:
        """检查A/B文件是否存在"""
        print(f"[1/4] 检查数据文件...")
        print(f"      📄 A文件(全量): {self.file_a.name}")
        print(f"      📄 B文件(已使用): {self.file_b.name}")

        if not self.file_a.exists():
            print(f"❌ 错误: A文件不存在 - {self.file_a}")
            return False
        if not self.file_b.exists():
            print(f"❌ 错误: B文件不存在 - {self.file_b}")
            return False

        return True

    def extract_oem_numbers(self, text: str) -> set[str]:
        """从文本中提取所有OEM号"""
        if pd.isna(text) or not str(text).strip():
            return set()

        normalized = self.OEM_SEPARATORS.sub(',', str(text))
        oems = {o.strip() for o in normalized.split(',') if o.strip()}
        return oems

    def load_and_process(self) -> pd.DataFrame | None:
        """加载数据并筛选"""
        # 加载B表
        print(f"[2/4] 加载B表并提取已使用OEM...")
        df_b = pd.read_excel(self.file_b)
        df_b.columns = df_b.columns.str.strip()

        # HTML清理
        print(f"[2.5/4] 清理B表HTML描述...")
        df_b = self.clean_html_columns(df_b)
        if self.stats['html_cleaned'] > 0:
            print(f"      ✅ 已清理 {self.stats['html_cleaned']} 个HTML单元格")

        if self.oem_col_b not in df_b.columns:
            print(f"❌ 错误: B表缺少列 '{self.oem_col_b}'，可用列: {list(df_b.columns)}")
            return None

        # 收集已使用OEM
        used_oems = set()
        for val in df_b[self.oem_col_b].dropna():
            used_oems.update(self.extract_oem_numbers(str(val)))

        self.stats['b_oem_count'] = len(used_oems)
        print(f"      已使用OEM总数: {len(used_oems)}")

        # 加载A表
        print(f"[3/4] 加载A表并筛选...")
        df_a = pd.read_excel(self.file_a)
        df_a.columns = df_a.columns.str.strip()
        self.stats['total_a_rows'] = len(df_a)

        # 动态检测VBS列
        self.vbs_col = self._detect_vbs_column(list(df_a.columns))

        if self.vbs_col:
            # ===== VBS模式：按VBS分组筛选 =====
            self.mode = "vbs"
            self.stats['mode'] = "VBS分组模式"
            print(f"      ✓ 检测到VBS列: '{self.vbs_col}'")
            return self._process_vbs_mode(df_a, used_oems)
        else:
            # ===== OEM模式：直接按OEM行筛选 =====
            self.mode = "oem"
            self.stats['mode'] = "OEM行模式"
            print(f"      ⚠️ 未检测到VBS列，切换到OEM行筛选模式")
            return self._process_oem_mode(df_a, used_oems)

    def _process_vbs_mode(self, df_a: pd.DataFrame, used_oems: set) -> pd.DataFrame:
        """VBS分组筛选模式"""
        if self.oem_col_a not in df_a.columns:
            print(f"❌ 错误: A表缺少列 '{self.oem_col_a}'，可用列: {list(df_a.columns)}")
            return None

        unused_rows = []
        total_vbs_groups = df_a.groupby(self.vbs_col, sort=False)
        self.stats['total_vbs'] = len(total_vbs_groups)

        for vbs_no, group in total_vbs_groups:
            vbs_oems = set()
            for oem_val in group[self.oem_col_a]:
                vbs_oems.update(self.extract_oem_numbers(str(oem_val)))

            if vbs_oems & used_oems:
                self.stats['used_vbs'] += 1
            else:
                unused_rows.append(group)

        if unused_rows:
            result_df = pd.concat(unused_rows, ignore_index=True)
            self.stats['unused_vbs'] = len(result_df.groupby(self.vbs_col))
            self.stats['output_rows'] = len(result_df)
        else:
            result_df = df_a.iloc[0:0].copy()
            self.stats['unused_vbs'] = 0

        print(f"      总VBS数: {self.stats['total_vbs']}")
        print(f"      已使用VBS: {self.stats['used_vbs']}")
        print(f"      未使用VBS: {self.stats['unused_vbs']}")
        return result_df

    def _process_oem_mode(self, df_a: pd.DataFrame, used_oems: set) -> pd.DataFrame:
        """OEM行筛选模式（无VBS列时）"""
        if self.oem_col_a not in df_a.columns:
            # 尝试找OEM相关列
            for col in df_a.columns:
                if 'oem' in col.lower():
                    self.oem_col_a = col
                    break
            else:
                print(f"❌ 错误: A表缺少OEM列，可用列: {list(df_a.columns)}")
                return None

        print(f"      ✓ 使用OEM列: '{self.oem_col_a}'")

        # 逐行检查OEM是否已使用
        unused_mask = []
        for _, row in df_a.iterrows():
            oem_val = str(row.get(self.oem_col_a, ''))
            row_oems = self.extract_oem_numbers(oem_val)

            # 如果该行所有OEM都未使用，则保留
            if row_oems and not (row_oems & used_oems):
                unused_mask.append(True)
            else:
                unused_mask.append(False)

        result_df = df_a[unused_mask].copy()
        self.stats['unused_vbs'] = len(result_df)  # 这里实际是未使用OEM行数
        self.stats['output_rows'] = len(result_df)

        print(f"      总行数: {len(df_a)}")
        print(f"      未使用OEM行: {len(result_df)}")
        print(f"      已使用/重复OEM行: {len(df_a) - len(result_df)}")
        return result_df

    def save_result(self, df: pd.DataFrame) -> Path | None:
        """保存结果"""
        if df.empty:
            print(f"⚠️  警告: 结果为空，不生成文件")
            return None

        if self.mode == "vbs":
            output_file = self.folder / f"{self.product_name}_未使用VBS.xlsx"
        else:
            output_file = self.folder / f"{self.product_name}_未使用OEM.xlsx"

        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"      💾 保存: {output_file.name} ({len(df)} 行)")
        return output_file

    def print_report(self):
        """打印统计报告"""
        print(f"\n{'=' * 60}")
        print("📊 OEM使用状态筛选报告")
        print(f"{'=' * 60}")
        print(f"  产品目录: {self.product_name}")
        print(f"  筛选模式: {self.stats.get('mode', 'unknown')}")
        print(f"  A表总行数: {self.stats['total_a_rows']}")
        print(f"  B表已使用OEM: {self.stats['b_oem_count']}")
        if self.stats['html_cleaned'] > 0:
            print(f"  🧹 HTML清理: {self.stats['html_cleaned']} 个单元格")
        print(f"-" * 60)

        if self.mode == "vbs":
            print(f"  ✅ 已使用VBS: {self.stats['used_vbs']} 个")
            print(f"  🆕 未使用VBS: {self.stats['unused_vbs']} 个")
            if self.stats['total_vbs'] > 0:
                unused_ratio = self.stats['unused_vbs'] / self.stats['total_vbs'] * 100
                print(f"  📈 未使用率: {unused_ratio:.1f}%")
        else:
            print(f"  🆕 未使用OEM行: {self.stats['unused_vbs']} 行")
            print(f"  ✅ 已使用/重复: {self.stats['total_a_rows'] - self.stats['unused_vbs']} 行")
            if self.stats['total_a_rows'] > 0:
                unused_ratio = self.stats['unused_vbs'] / self.stats['total_a_rows'] * 100
                print(f"  📈 未使用率: {unused_ratio:.1f}%")

        print(f"  📄 输出记录: {self.stats['output_rows']} 行")
        print(f"{'=' * 60}")

    def run(self) -> None:
        """主处理流程"""
        print(f"\n===== OEM使用状态筛选 =====")
        print(f"📂 工作目录: {self.folder}")

        if not self.check_files():
            return

        result_df = self.load_and_process()
        if result_df is None:
            return

        print(f"[4/4] 保存结果...")
        self.save_result(result_df)
        self.print_report()
        print("✅ 筛选完成！")