import os
import glob
import json
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_substrings(csv_path):
    substrings = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            substrings.append({
                'split': row['split'],
                'instance_id': row['instance_id'],
                'sub_string': row['sub_string']
            })
    return substrings

def process_file(json_path, substrings):
    print(f"Handle {json_path}")
    results = []
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    for i, item in enumerate(data):
        if i != 0 and i % 10 == 0:
            print(f"{json_path} processed {i} conversations")
        hit = False
        for conv in item.get('conversations', []):
            content = conv.get('content', '')
            for sub in substrings:
                if sub['sub_string'] in content:
                    results.append((sub['split'], sub['instance_id']))
                    hit = True
                    break
            if hit:
                continue
    return results

def main(thread_num=4):
    substrings = load_substrings('swe_sub_strings.csv')
    json_files = glob.glob('chunk_*pr.json')
    with ThreadPoolExecutor(max_workers=thread_num) as executor:
        futures = {executor.submit(process_file, jf, substrings): jf for jf in json_files}
        for future in as_completed(futures):
            for split, instance_id in future.result():
                print(split, instance_id)

if __name__ == '__main__':
    import sys
    thread_num = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    main(thread_num)