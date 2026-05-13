from pathlib import Path

# 修改成你的两个大文件夹路径
folder1 = Path(r"\\A\g\ebay 图片未处理\Air_filter_HBKS(5)2\Air_filter_HBKS(5)2原")
folder2 = Path(r"C:\Users\Administrator\PyCharmMiscProject\图片查重代码\data_output\cooling_fan_CZXCD(7)")

# 获取第一层子文件夹名称
subfolders1 = {f.name for f in folder1.iterdir() if f.is_dir()}
subfolders2 = {f.name for f in folder2.iterdir() if f.is_dir()}

# 找出差异
only_in_1 = subfolders1 - subfolders2
only_in_2 = subfolders2 - subfolders1

print("===== FolderA 有，但 FolderB 没有 =====")
for name in sorted(only_in_1):
    print(name)

print("\n===== FolderB 有，但 FolderA 没有 =====")
for name in sorted(only_in_2):
    print(name)

print("\n对比完成 ✅")