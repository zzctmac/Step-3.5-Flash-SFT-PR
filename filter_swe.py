
import os
import glob
import json
import csv
import ahocorasick
from concurrent.futures import ProcessPoolExecutor, as_completed

def build_automaton(csv_path):
    A = ahocorasick.Automaton()
    meta = {}
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sub = row['sub_string']
            idx = len(meta)
            A.add_word(sub, idx)
            meta[idx] = (row['split'], row['instance_id'], sub)
    A.make_automaton()
    return A, meta

def process_file(json_path, csv_path):
    # 每个进程独立构建ac自动机，避免多进程共享问题
    print(f"Processing {json_path} with {csv_path}")
    A, meta = build_automaton(csv_path)
    filtered_data = []
    removed_count = 0
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    for i, item in enumerate(data):
        hit = False
        for conv in item.get('conversations', []):
            content = conv.get('content', '')
            for end_idx, idx in A.iter(content):
                # 命中则标记
                hit = True
                break
            if hit:
                break
        if not hit:
            filtered_data.append(item)
        else:
            removed_count += 1
    # 写回过滤后的数据
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    return removed_count


def main(proc_num=4):
    csv_path = 'swe_sub_strings.csv'
    json_files = glob.glob('chunk_*pr.json')
    total_removed = 0
    with ProcessPoolExecutor(max_workers=proc_num) as executor:
        futures = {executor.submit(process_file, jf, csv_path): jf for jf in json_files}
        for future in as_completed(futures):
            removed = future.result()
            total_removed += removed
    print(f"Total removed items: {total_removed}")

if __name__ == '__main__':
    import sys
    proc_num = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    main(proc_num)