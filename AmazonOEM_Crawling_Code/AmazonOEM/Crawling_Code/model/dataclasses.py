from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass
class PageTask:
    """
    页面任务数据包
    传递HTML文件路径，消费者独立读取
    """
    url: str
    html_file: Path  # HTML文件路径，而非page对象
    title: str = ''
    price: str = ''
    idx: int = 0
    WBS_No: str = ''  # 添加这个
    oem_No: str = ''
    def __repr__(self) -> str:
        return f"PageTask(idx={self.idx}, file={self.html_file.name})"