import os


def count_all_images(main_folder):
    """
    统计指定文件夹及其所有子文件夹中的图片总数

    Args:
        main_folder (str): 要统计的主文件夹路径

    Returns:
        int: 图片总数
    """
    # 定义常见的图片扩展名（包含大小写）
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg'}

    # 初始化计数器
    total_images = 0

    # 遍历文件夹
    try:
        # walk会递归遍历所有子文件夹
        for root, dirs, files in os.walk(main_folder):
            # 遍历当前文件夹中的所有文件
            for file in files:
                # 获取文件扩展名（转为小写）
                file_ext = os.path.splitext(file)[1].lower()
                # 判断是否为图片文件
                if file_ext in image_extensions:
                    total_images += 1
                    # 可选：打印每个图片的路径，方便核对
                    # print(f"找到图片: {os.path.join(root, file)}")

        return total_images

    except FileNotFoundError:
        print(f"错误：找不到文件夹 '{main_folder}'")
        return 0
    except PermissionError:
        print(f"错误：没有访问文件夹 '{main_folder}' 的权限")
        return 0
    except Exception as e:
        print(f"发生未知错误：{str(e)}")
        return 0


# 主程序
if __name__ == "__main__":
    # ========== 请修改这里的路径 ==========
    # Windows路径示例: r"C:\Users\你的名字\Pictures\我的图片文件夹"
    # Mac/Linux路径示例: "/Users/你的名字/Pictures/我的图片文件夹"
    target_folder = r"\\A\g\数据图片未处理\shocks_absorber_ZJKLS(5)\shocks_absorber_ZJKLS(5)"

    # 检查路径是否填写正确
    if target_folder == r"请替换为你的文件夹路径":
        print("请先修改代码中的 'target_folder' 变量，填写你要统计的文件夹路径！")
    else:
        # 统计图片数量
        image_count = count_all_images(target_folder)

        # 输出结果
        print(f"\n统计完成！")
        print(f"文件夹 '{target_folder}' 及其所有子文件夹中共有 {image_count} 张图片")