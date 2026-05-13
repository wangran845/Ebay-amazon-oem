from core.pipeline import Pipeline,Pipeline_1
from workers.picture import PictureDownloader #图片下载
from core.url_deduplicator import EbayURLDeduplicator #去重

if __name__ == "__main__":
    Pipeline().run() #爬详情页

    EbayURLDeduplicator.run() #去重url

    Pipeline_1().run() #数据清洗

    PictureDownloader().run() #下载图片

