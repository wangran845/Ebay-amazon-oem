from pathlib import Path

# 修改成你的两个大文件夹路径
folder1 = Path(r"H:\数据图片未处理\Adblue_Pump_NBLJ(2)已清洗\Adblue_Pump_NBLJ(2)少了1个文件夹-卓蕊文-26.3.9")
folder2 = Path(r"H:\数据图片未处理\Adblue_Pump_NBLJ(2)已清洗\Adblue_Pump_NBLJ(2)")

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