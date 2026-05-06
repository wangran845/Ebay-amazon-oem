"""
页面队列类 - 传递HTML文件路径而非page对象
"""
from __future__ import annotations
from queue import Queue, Empty, Full
from dataclasses import dataclass
from pathlib import Path
from model.dataclasses import PageTask

class PageQueue:
    """线程安全的页面队列"""

    POISON_PILL = None

    def __init__(self, max_size: int = 10):
        self._queue: Queue = Queue(maxsize=max_size)
        self._max_size = max_size

    def put(self, task: PageTask, block: bool = True, timeout: float = None) -> bool:
        try:
            self._queue.put(task, block=block, timeout=timeout)
            return True
        except Full:
            return False

    def get(self, block: bool = True, timeout: float = None):
        """获取任务，超时抛Empty异常，不要返回None"""
        return self._queue.get(block=block, timeout=timeout)
        # 不要try-except，让Empty异常抛出去！

    def task_done(self) -> None:
        self._queue.task_done()

    def join(self) -> None:
        self._queue.join()

    def size(self) -> int:
        return self._queue.qsize()

    def send_poison(self, num_consumers: int) -> None:
        for _ in range(num_consumers):
            self._queue.put(self.POISON_PILL)

    def is_full(self) -> bool:
        return self._queue.full()

    def is_empty(self) -> bool:
        return self._queue.empty()