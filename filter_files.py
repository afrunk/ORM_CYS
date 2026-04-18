import os
from datetime import datetime

def filter_old_files(target_dir="/root/ORM/static/uploads", deadline_date=datetime(2026, 1, 23)):
    """
    扫描目录，返回所有文件名以 YYYYMMDD 开头且日期早于截止日期的文件列表。
    """
    print(f"--- 正在扫描目录: {target_dir} ---")
    print(f"--- 筛选目标: {deadline_date.strftime('%Y-%m-%d')} 之前的文件 ---\n")

    if not os.path.exists(target_dir):
        print("错误：目录不存在，请检查路径。")
        return []

    found_files = []
    total_scanned = 0

    for filename in os.listdir(target_dir):
        file_path = os.path.join(target_dir, filename)
        if not os.path.isfile(file_path):
            continue

        total_scanned += 1

        try:
            # 文件名格式: 20260323_190127_...
            date_str = filename.split('_')[0]
            file_date = datetime.strptime(date_str, "%Y%m%d")

            if file_date < deadline_date:
                found_files.append(filename)
                print(f"[匹配] {filename} (日期: {file_date.strftime('%Y-%m-%d')})")

        except (ValueError, IndexError):
            # 忽略不符合命名规范的文件
            continue

    print(f"\n--- 统计结果 ---")
    print(f"总计扫描文件数: {total_scanned}")
    print(f"匹配到待删除文件数: {len(found_files)}")
    return found_files


def delete_files(file_list, target_dir):
    """
    删除指定目录中的文件列表。
    """
    if not file_list:
        print("没有文件需要删除。")
        return

    deleted = 0
    failed = 0

    for filename in file_list:
        file_path = os.path.join(target_dir, filename)
        try:
            os.remove(file_path)
            print(f"[已删除] {filename}")
            deleted += 1
        except Exception as e:
            print(f"[删除失败] {filename}: {e}")
            failed += 1

    print(f"\n--- 删除结果 ---")
    print(f"成功删除: {deleted} 个文件")
    print(f"删除失败: {failed} 个文件")


if __name__ == "__main__":
    # 可在此修改目录和截止日期
    TARGET_DIR = "/root/ORM/static/uploads"
    DEADLINE = datetime(2026, 1, 23)

    old_files = filter_old_files(TARGET_DIR, DEADLINE)

    if old_files:
        print("\n" + "="*50)
        print("⚠️  即将删除以上列出的所有文件，操作不可恢复！")
        confirm = input("确认要删除这些文件吗？(y/N): ").strip().lower()
        if confirm == 'y':
            delete_files(old_files, TARGET_DIR)
        else:
            print("操作已取消，未删除任何文件。")
    else:
        print("\n未发现需要删除的文件。")