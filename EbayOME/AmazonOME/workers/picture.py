"""
图片下载器 - 自动保留最高频Category
"""
from __future__ import annotations
import os
import re
import time
import uuid
import requests
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class PictureDownloader:
    """
    图片下载器类 - 自动保留最高频Category
    """

    def __init__(self, config=None):
        if config is None:
            try:
                from config.config import Config
                config = Config()
            except ImportError:
                raise ImportError("无法导入Config类")

        self.config = config

        # 路径配置
        self.excel_path = Path(config.picture_excel_path)
        self.output_dir = Path(config.picture_output_dir)

        # 列名配置
        self.picture_col = getattr(config, 'PICTURE_COL', 'picture')
        self.ome_col = getattr(config, 'PICTURE_OME_COL', 'OME')
        self.category_col = getattr(config, 'CATEGORY_COL', 'category')  # category列名

        # 线程配置
        self.max_workers = getattr(config, 'PICTURE_MAX_WORKERS', 5)
        self.timeout = getattr(config, 'PICTURE_TIMEOUT', 30)
        self.retry_times = getattr(config, 'PICTURE_RETRY_TIMES', 3)
        self.delay = getattr(config, 'PICTURE_DELAY', 0.5)

        # 统计
        self.stats = {
            'total_rows': 0,
            'filtered_rows': 0,
            'top_category': None,  # 记录保留的category
            'total_images': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }

        # 锁机制
        self._lock = threading.Lock()
        self._ome_locks = {}
        self._ome_locks_lock = threading.Lock()
        self._ome_counters = {}

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36'
        }

    def _filter_by_top_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        保留出现频次最高的category数据

        Args:
            df: 原始DataFrame

        Returns:
            筛选后的DataFrame（只保留最高频category）
        """
        if self.category_col not in df.columns:
            available = df.columns.tolist()
            raise ValueError(f"未找到category列 '{self.category_col}'，可用列: {available}")

        original_count = len(df)
        print(f"\n[筛选] 原始数据: {original_count} 行")
        print(f"  分析列: {self.category_col}")

        # 统计category频次（处理空值）
        # 先清理数据：转字符串、去空格、转小写统一比较
        category_series = df[self.category_col].astype(str).str.strip()

        # 过滤掉空值和nan字符串
        valid_categories = category_series[
            (category_series != '') &
            (category_series.str.lower() != 'nan') &
            (category_series != 'None')
            ]

        if len(valid_categories) == 0:
            raise ValueError(f"category列 '{self.category_col}' 无有效数据")

        # 统计频次（区分大小写精确匹配，但显示原始值）
        category_counts = Counter(valid_categories)

        print(f"\n  [Category分布]:")
        for cat, count in category_counts.most_common(10):
            percentage = count / len(valid_categories) * 100
            marker = " ← 保留" if count == category_counts.most_common(1)[0][1] else ""
            print(f"    {cat}: {count} ({percentage:.1f}%){marker}")

        # 获取最高频category（如果有多个相同频次，取第一个出现的）
        top_category = category_counts.most_common(1)[0][0]
        top_count = category_counts.most_common(1)[0][1]

        self.stats['top_category'] = top_category

        # 精确匹配保留（区分大小写，严格匹配）
        mask = category_series == top_category
        filtered_df = df[mask].copy()
        filtered_count = len(filtered_df)

        print(f"\n  保留Category: '{top_category}'")
        print(f"  保留行数: {filtered_count} / {original_count}")
        print(
            f"  过滤掉: {original_count - filtered_count} 行 ({(original_count - filtered_count) / original_count * 100:.1f}%)")

        self.stats['filtered_rows'] = filtered_count
        return filtered_df

    def _get_ome_lock(self, ome: str) -> threading.Lock:
        with self._ome_locks_lock:
            if ome not in self._ome_locks:
                self._ome_locks[ome] = threading.Lock()
            return self._ome_locks[ome]

    def _get_next_image_number(self, ome: str) -> int:
        with self._lock:
            self._ome_counters[ome] = self._ome_counters.get(ome, 0) + 1
            return self._ome_counters[ome]

    def _parse_picture_urls(self, picture_cell: str) -> List[str]:
        if pd.isna(picture_cell) or not str(picture_cell).strip():
            return []
        urls = [url.strip() for url in str(picture_cell).split('\n') if url.strip()]
        return [url for url in urls if url.startswith(('http://', 'https://'))]

    def _sanitize_ome(self, ome: str) -> str:
        if pd.isna(ome):
            return "unknown"
        ome_str = str(ome).strip()
        ome_str = re.sub(r'[<>:"/\\|?*]', '_', ome_str)
        return ome_str[:50] if ome_str else "unknown"

    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        url_lower = url.lower()
        for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            if ext in url_lower:
                return '.jpg' if ext == '.jpeg' else ext

        if content_type:
            if 'jpeg' in content_type or 'jpg' in content_type:
                return '.jpg'
            elif 'png' in content_type:
                return '.png'
            elif 'gif' in content_type:
                return '.gif'
            elif 'webp' in content_type:
                return '.webp'
        return '.jpg'

    def _find_existing_file(self, folder_path: Path, file_stem: str) -> Optional[Path]:
        for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            candidate = folder_path / (file_stem + ext)
            if candidate.exists():
                return candidate
        return None

    def _download_single_image(
            self,
            url: str,
            folder_path: Path,
            file_stem: str,
            ome: str
    ) -> Tuple[bool, str]:
        ome_lock = self._get_ome_lock(ome)

        with ome_lock:
            existing = self._find_existing_file(folder_path, file_stem)
            if existing:
                with self._lock:
                    self.stats['skipped'] += 1
                rel_path = existing.relative_to(self.output_dir).as_posix()
                print(f"  [跳过] {ome}/{existing.name} 已存在")
                return True, rel_path

        temp_name = f".tmp.{file_stem}.{uuid.uuid4().hex[:8]}"
        temp_path = folder_path / temp_name
        final_ext = '.jpg'
        download_success = False

        for attempt in range(self.retry_times):
            try:
                session = requests.Session()
                response = session.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    stream=True
                )
                response.raise_for_status()

                content_type = response.headers.get('Content-Type', '')
                final_ext = self._get_file_extension(url, content_type)

                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                session.close()
                download_success = True
                break

            except Exception as e:
                error_msg = str(e)
                if attempt < self.retry_times - 1:
                    time.sleep(1)
                else:
                    if temp_path.exists():
                        temp_path.unlink()
                    with self._lock:
                        self.stats['failed'] += 1
                    print(f"  [失败] {ome}/{file_stem} - {error_msg[:50]}")
                    return False, error_msg

        if not download_success:
            return False, "下载失败"

        final_path = folder_path / (file_stem + final_ext)

        with ome_lock:
            existing = self._find_existing_file(folder_path, file_stem)
            if existing:
                temp_path.unlink()
                with self._lock:
                    self.stats['skipped'] += 1
                rel_path = existing.relative_to(self.output_dir).as_posix()
                print(f"  [跳过] {ome}/{existing.name} 已存在（下载完成后发现）")
                return True, rel_path

            try:
                temp_path.rename(final_path)
                with self._lock:
                    self.stats['success'] += 1

                file_size = final_path.stat().st_size // 1024
                print(f"  [成功] {ome}/{final_path.name} ({file_size}KB)")

            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                with self._lock:
                    self.stats['failed'] += 1
                return False, f"重命名失败: {e}"

        if self.delay > 0:
            time.sleep(self.delay)

        rel_path = final_path.relative_to(self.output_dir).as_posix()
        return True, rel_path

    def _process_row(self, row_data: Dict) -> Dict:
        index = row_data['index']
        ome = row_data['ome']
        urls = row_data['urls']

        folder_name = self._sanitize_ome(ome)
        folder_path = self.output_dir / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)

        downloaded_paths = []

        for url in urls:
            ome_lock = self._get_ome_lock(folder_name)
            with ome_lock:
                n = self._get_next_image_number(folder_name)

            file_stem = f"{folder_name}-{n}"

            success, result = self._download_single_image(
                url, folder_path, file_stem, folder_name
            )

            if success:
                downloaded_paths.append(result)

        return {
            'index': index,
            'ome': ome,
            'total': len(urls),
            'downloaded': len(downloaded_paths),
            'paths': downloaded_paths
        }

    def run(self, save_excel: bool = True, output_excel_path: Optional[str] = None) -> pd.DataFrame:
        # 👇 加在 read_excel 前面
        if not os.path.exists(self.excel_path):
            print(f"[图片下载器] 无数据，跳过：{self.excel_path}")
            return
        print("=" * 60)
        print("图片下载器启动 (自动保留最高频Category)")
        print("=" * 60)
        print(f"Excel文件: {self.excel_path}")
        print(f"输出目录: {self.output_dir}")
        print(f"并发线程: {self.max_workers}")
        print("-" * 60)

        # 读取Excel
        print("\n[1/5] 读取Excel文件...")
        try:
            df = pd.read_excel(self.excel_path)
            self.stats['total_rows'] = len(df)
            print(f"  成功读取 {len(df)} 行数据")
            print(f"  可用列: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"  [错误] 读取Excel失败: {e}")
            raise

        # 检查必要列
        if self.picture_col not in df.columns:
            raise ValueError(f"未找到列 '{self.picture_col}'")
        if self.ome_col not in df.columns:
            raise ValueError(f"未找到列 '{self.ome_col}'")

        # 自动筛选最高频Category
        print("\n[2/5] 自动筛选最高频Category...")
        df = self._filter_by_top_category(df)

        if len(df) == 0:
            print("  [警告] 筛选后无数据，程序结束")
            return df

        # 准备任务
        print("\n[3/5] 解析图片链接...")
        tasks = []
        for idx, row in df.iterrows():
            urls = self._parse_picture_urls(row[self.picture_col])
            if urls:
                self.stats['total_images'] += len(urls)
                tasks.append({
                    'index': idx,
                    'ome': row[self.ome_col],
                    'urls': urls
                })

        print(f"  找到 {len(tasks)} 行包含图片链接")
        print(f"  共 {self.stats['total_images']} 张待下载图片")

        # 并发下载
        print("\n[4/5] 开始下载图片...")
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self._process_row, task): task
                for task in tasks
            }

            for future in as_completed(future_to_task):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"  [错误] 处理任务失败: {e}")

        # 更新DataFrame
        print("\n[5/5] 更新Excel数据...")
        image_paths_column = []
        for idx in range(len(df)):
            result = next((r for r in results if r['index'] == idx), None)
            if result and result['paths']:
                image_paths_column.append('\n'.join(result['paths']))
            else:
                image_paths_column.append('')

        df['image_paths'] = image_paths_column

        # 保存Excel
        if save_excel:
            if output_excel_path:
                save_path = Path(output_excel_path)
            else:
                stem = self.excel_path.stem
                suffix = self.excel_path.suffix
                # 文件名包含保留的category
                safe_cat = re.sub(r'[<>:"/\\|?*]', '_', str(self.stats['top_category']))[:30]
                save_path = self.excel_path.parent / f"{stem}_with_images{suffix}"

            df.to_excel(save_path, index=False)
            print(f"  已保存: {save_path}")

        # 统计
        print("\n" + "=" * 60)
        print("下载完成统计")
        print("=" * 60)
        print(f"原始行数:     {self.stats['total_rows']}")
        print(f"保留Category: {self.stats['top_category']}")
        print(f"保留行数:     {self.stats['filtered_rows']}")
        print(f"待下载图片:   {self.stats['total_images']}")
        print(f"下载成功:     {self.stats['success']}")
        print(f"跳过(已存在): {self.stats['skipped']}")
        print(f"下载失败:     {self.stats['failed']}")
        print("=" * 60)

        return df


def main():
    PictureDownloader().run()


if __name__ == '__main__':
    main()