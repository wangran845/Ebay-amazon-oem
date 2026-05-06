from __future__ import annotations
import pandas as pd
from bs4 import BeautifulSoup
from data.config import Config
from pathlib import Path


class KLbt7274:

    def __init__(self,config: Config =None ):
        self.config = config if config else Config()
        self.product_user_2 = self.config.product_user_2

        #路径
        self.src_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_已格式化.xlsx")
        self.dst_file = Path(fr"data/{self.product_user_2}/{self.product_user_2}_KL.xlsx")

    # HTML转TXT（带换行）
    def html_to_text_with_newline(self,html):
        if pd.isna(html):
            return ""

        soup = BeautifulSoup(str(html), "html.parser")

        # 提取所有文本
        texts = [t.strip() for t in soup.stripped_strings]

        # 用换行符连接
        return "\n".join(texts)

    def run(self) -> None:
        # 读取Excel
        df = pd.read_excel(self.src_file)

        # D列 → K列
        df["K"] = df.iloc[:, 3].apply(self.html_to_text_with_newline)

        # E列 → L列
        df["L"] = df.iloc[:, 4].apply(self.html_to_text_with_newline)

        # 保存Excel
        output_file = self.dst_file
        df.to_excel(output_file, index=False)
        print("转换完成，保存为:", output_file)


def main():
    cleaner = KLbt7274()
    cleaner.run()


if __name__ == "__main__":
    main()