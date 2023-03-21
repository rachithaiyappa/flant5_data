import subprocess
import gzip
import json
import pandas as pd
import sys


df_out = pd.DataFrame()
# raw
# df = pd.read_csv("/nobackup/racball/github/beliefnet/notebooks/20220723_ZeroShot/data/testdata-taskB-all-annotations.txt",sep="\t")
# preprocessed
df = pd.read_parquet("/nobackup/racball/github/beliefnet/notebooks/20220723_ZeroShot/data/preprocessed/test_preprocessed_taskA.parquet")
texts = set(df.Tweet)

print(sys.argv)

start = int(sys.argv[1])
finish = int(sys.argv[2])


ids,out = [],[]
for i in range(start,finish):
    try:
        ids.append(i)
        label = str(i).zfill(5)
        print(label)
        # subprocess.run(["git", "lfs", "pull", "--include", f"en/c4-train.{label}-of-01024.json.gz"])
        jsonfilename = f'/data2/sg/racball/c4/en/c4-train.{label}-of-01024.json.gz'
        with gzip.open(jsonfilename, 'r') as fin:
            data = [json.loads(line)['text'] for line in fin]

        out.append(texts.intersection(data))
        # subprocess.run(["rm", "-rf", f"en/c4-train.{label}-of-01024.json.gz"])

    except FileNotFoundError:
        continue
df_out['id'] = ids
df_out['intersection'] = out
df_out.to_parquet(f"{sys.argv[3]}")
# df_out.to_parquet(f"chunk_{start}_{finish}")