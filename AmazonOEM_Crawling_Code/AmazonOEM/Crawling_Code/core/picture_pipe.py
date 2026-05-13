from workers.picture import PictureDownloader


class picturedown:

    def run(self):
        # 创建下载器实例
        downloader = PictureDownloader(
            excel_path='data/output/lest_but_not_list.xlsx',
            output_dir='E:/oem_picture',
            picture_col='picture',
            oem_col='oem',
            max_workers=5,      # 并发线程数
            timeout=30,         # 超时时间
            retry_times=3,      # 重试次数
            delay=0.5           # 下载间隔
        )

        # 执行下载
        df = downloader.run(save_excel=True)