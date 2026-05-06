import os.path
import os
import shutil
import hashlib
import pandas as pd

if __name__ == "__main__":
    dir_path = "turbocharger_WXBSWE(5)"
    excel = pd.read_excel(
        os.path.join(r"E:\AmazonOMETT\data", dir_path,
                     f"{dir_path}_已格式化.xlsx"))
    print(len(excel["OEM"]))
    print(os.path.join(r"E:\ome_picture", dir_path))
    for root, dirs, files in os.walk(os.path.join(r"E:\ome_picture", dir_path)):
        oem = root.split("\\")[-1]
        if oem not in excel['OEM'].tolist() and oem != dir_path:
            print(root)
            shutil.rmtree(root)
