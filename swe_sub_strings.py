from datasets import load_dataset
import random
import csv
import pandas as pd

random.seed(42)

def random_substrings(s):
    substr_len = min(len(s), 100)
    ic = 1
    if len(s) <= substr_len:
        return [s] * ic
    result = [s[0:0 + substr_len]]
    return result

sbf = load_dataset('SWE-bench/SWE-bench', download_mode="reuse_dataset_if_exists")

data_origin = {
    'split':[],
    'instance_id':[],
    'sub_string':[]
}


for split in ['test', 'dev', 'train']:
    for item in sbf[split]:
        substrings = random_substrings(item['problem_statement'])
        for sub in substrings:
            data_origin['split'].append(split)
            data_origin['instance_id'].append(item['instance_id'])
            data_origin['sub_string'].append(sub)

data_pd = pd.DataFrame(data_origin)
data_pd.to_csv('swe_sub_strings.csv', index=False)