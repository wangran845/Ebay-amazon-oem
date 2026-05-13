from __future__ import annotations
from pathlib import Path


class Config:
    product_key = "DRV_valve_HXH(9)_VBS"
    output_2 = f"{product_key}_need.xlsx"

    def __init__(self, output_2=None, product_key=None):
        # 如果没传参，使用类默认值
        self.product_key = product_key or self.__class__.product_key
        self.output_2 = output_2 or f"{self.product_key}_need.xlsx"

        self.input_1 = f'{self.product_key}.xlsx'
        self.output_1 = "result.xlsx"
        self.input_2 = self.output_1

    @property
    def input_file(self) -> Path:
        return Path(f"../Crawling_Code/data/input/{self.input_1}")

    @property
    def input_file_de(self) -> Path:
        return Path(f"../Crawling_Code/data/output/{self.input_2}")

    @property
    def output_file(self) :
        return f'data/output/{self.input_2}'

    @property
    def output_file_de(self):
        return f'data/output/{self.output_2}'







    PRODUCER_NUM: int = 1
    CONSUMER_NUM: int = 3
    QUEUE_MAX_SIZE: int = 10

    #HTML缓存目录（即用即删）
    @property
    def html_cache_dir(self) -> Path:
        path = Path(f'cache')
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def html_cache_dir_1(self) -> Path:
        path = Path(f'cache_1')
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def html_cache_dir_2(self) -> Path:
        path = Path(f'cache_2')
        path.mkdir(parents=True, exist_ok=True)
        return path

        # 浏览器配置
    DEBUG_PORT: str = '9222'
    USER_DATA_DIR: str = r'D:\temp\napa_001'
    PAGE_TIMEOUT: int = 10

    TEMP_FILE: Path  = Path('details_tmp_oem_url.xlsx')
    TEMP_FILE_DE:Path= Path('details_tmp_de.xlsx')
    MIN_DELAY:float = 0.1
    MAX_DELAY:float = 0.3

    OUTPUT_HEADERS:list[str] =["URL","OE/OEM"]
    OUTPUT_HEADERS_DE: list[str] = ["URL", "Title","price","About_table","item specifics",'description_from_the_seller',"seller_feedback",'category','picture','oem']


    def ensure_dirs(self) -> None:
        self.html_cache_dir.mkdir(parents=True, exist_ok=True)
        self.html_cache_dir_2.mkdir(parents=True, exist_ok=True)
        self.html_cache_dir_1.mkdir(parents=True, exist_ok=True)

    # ==================== 图片下载配置 ====================

    # 图片下载源Excel文件路径（包含picture和oem列的表格）
    PICTURE_EXCEL_SOURCE: str = f'data/output/{output_2}'

    # 图片下载输出目录
    PICTURE_OUTPUT_DIR: str = f'E:/oem_picture/{product_key}'

    # 图片链接列名
    PICTURE_COL: str = 'picture'

    # oem号列名
    PICTURE_oem_COL: str = 'oem'

    # 下载并发线程数
    PICTURE_MAX_WORKERS: int = 6

    # 下载超时时间（秒）
    PICTURE_TIMEOUT: int = 30

    # 失败重试次数
    PICTURE_RETRY_TIMES: int = 3

    # 下载间隔延迟（秒）
    PICTURE_DELAY: float = 0.5

    @property
    def picture_excel_path(self) -> Path:
        """图片下载源Excel文件路径"""
        return Path(self.PICTURE_EXCEL_SOURCE)

    @property
    def picture_output_dir(self) -> Path:
        """图片下载输出目录"""
        path = Path(self.PICTURE_OUTPUT_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path