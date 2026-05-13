"""
生产者类 - 获取页面，保存HTML，传递文件路径
"""
from __future__ import annotations
import threading
from typing import List, Tuple

from core.page_queue import PageQueue
from model.dataclasses import PageTask

class Producer(threading.Thread):
    """
    生产者线程
    职责：读取URL -> 获取页面 -> 保存HTML -> 传递文件路径到队列
    """

    def __init__(self,
                 config1: 'Config',
                 fetcher1: 'Fetcher',
                 page_queue: PageQueue,
                 url_list: List[Tuple[str, str, str]]):
        super().__init__(name='Producer', daemon=True)
        self.config = config1
        self.fetcher = fetcher1
        self.queue = page_queue
        self.url_list = url_list

        self.total = len(url_list)
        self.processed = 0
        self.success = 0
        self.failed = 0

    def run(self) -> None:
        """主循环"""
        print(f'[Producer] 启动，共 {self.total} 个URL')

        for idx, (WBS_No,url,oem_No) in enumerate(self.url_list, 1):
            self.processed += 1

            print(f'\n[Producer] [{idx}/{self.total}] 正在获取: {url[:60]}...')

            # 获取并保存HTML
            html_file = self.fetcher.fetch_and_save(url, idx)

            if html_file is None:
                print(f'[Producer] ✗ 获取失败，跳过')
                self.failed += 1
                continue

            # 封装任务（传递文件路径）
            task = PageTask(
                url=url,
                html_file=html_file,
                WBS_No=WBS_No,
                oem_No=oem_No,
                idx=idx
            )

            # 放入队列（阻塞等待）
            while True:
                if self.queue.put(task, block=True, timeout=1):
                    break
                print(f'[Producer] 队列已满({self.queue.size()})，等待消费者...')

            self.success += 1
            print(f'[Producer] ✓ 已入队，队列长度: {self.queue.size()}')

            # 延迟
            if idx < self.total:
                self.fetcher.delay()

        print(f'[Producer] 全部完成，成功{self.success}，失败{self.failed}')



class Producer_1(threading.Thread):
    """
    生产者线程
    职责：读取URL -> 获取页面 -> 保存HTML -> 传递文件路径到队列
    """

    def __init__(self,
                 config: 'Config_1',
                 fetcher: 'Fetcher_1',
                 page_queue: PageQueue,
                 url_list: List[Tuple[str, str]]):
        super().__init__(name='Producer', daemon=True)
        self.config = config
        self.fetcher = fetcher
        self.queue = page_queue
        self.url_list = url_list

        self.total = len(url_list)
        self.processed = 0
        self.success = 0
        self.failed = 0

    def run(self) -> None:
        """主循环"""
        print(f'[Producer] 启动，共 {self.total} 个URL')

        for idx, (url,OEMNo) in enumerate(self.url_list, 1):
            self.processed += 1

            print(f'\n[Producer] [{idx}/{self.total}] 正在获取: {url[:60]}...')

            # 获取并保存HTML
            html_file = self.fetcher.fetch_and_save(url, idx)

            if html_file is None:
                print(f'[Producer] ✗ 获取失败，跳过')
                self.failed += 1
                continue

            # 封装任务（传递文件路径）
            task = PageTask(
                url=url,
                html_file=html_file,
                oem_No=OEMNo,
                idx=idx
            )

            # 放入队列（阻塞等待）
            while True:
                if self.queue.put(task, block=True, timeout=1):
                    break
                print(f'[Producer] 队列已满({self.queue.size()})，等待消费者...')

            self.success += 1
            print(f'[Producer] ✓ 已入队，队列长度: {self.queue.size()}')

            # 延迟
            if idx < self.total:
                self.fetcher.delay()

        print(f'[Producer] 全部完成，成功{self.success}，失败{self.failed}')