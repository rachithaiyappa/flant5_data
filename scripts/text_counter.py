import subprocess
import gzip
import json
import pandas as pd
import sys
from collections import Counter


df_out = pd.DataFrame()
# raw
# df = pd.read_csv("/nobackup/racball/github/beliefnet/notebooks/20220723_ZeroShot/data/testdata-taskB-all-annotations.txt",sep="\t")
# preprocessed
# df = pd.read_parquet("/nobackup/racball/github/beliefnet/notebooks/20220723_ZeroShot/data/preprocessed/test_preprocessed_taskA.parquet")
# texts = df.Tweet.to_list()

print(sys.argv)

start = int(sys.argv[1])
finish = int(sys.argv[2])


ids = []
true,false = [],[]
favor,against = [],[]
positive,negative = [],[]
entails,contradicts = [], []

Trues,Falses = [],[]
Favor,Against = [],[]
Positive,Negative = [],[]
Entails,Contradicts = [], []

print("chunk", start,finish)
for i in range(start,finish):
    try:
        ids.append(i)
        label = str(i).zfill(5)
        print(label)
        jsonfilename = f'/data2/sg/racball/c4/en/c4-train.{label}-of-01024.json.gz'
        with gzip.open(jsonfilename, 'r') as fin:
            data = [json.loads(line)['text'] for line in fin]

        data_ = [i.split() for i in data]
        data_ = [item for sublist in data_ for item in sublist]

        d = Counter(data_)

        true.append(d["true"])
        false.append(d["false"])
        positive.append(d["positive"])
        negative.append(d["negative"])
        favor.append(d["positive"])
        against.append(d["negative"])
        entails.append(d["entails"])
        contradicts.append(d["contradicts"])

        
        Trues.append(d["True"])
        Falses.append(d["False"])
        Favor.append(d["Favor"])
        Against.append(d["Against"])
        Positive.append(d["Positive"])
        Negative.append(d["Negative"])
        Entails.append(d["Entails"])
        Contradicts.append(d["Contradicts"])

    except FileNotFoundError:
        continue

df_out['id'] = ids

df_out['true'] = true
df_out['false'] = false
df_out['positive'] = positive
df_out['negative'] = negative
df_out['favor'] = favor
df_out['against'] = against
df_out['entails'] = entails
df_out['contradicts'] = contradicts


df_out['True'] = Trues
df_out['False'] = Falses
df_out['Positive'] = Positive
df_out['Negative'] = Negative
df_out['Favor'] = Favor
df_out['Against'] = Against
df_out['Entails'] = Entails
df_out['Contradicts'] = Contradicts

# df_out.to_parquet(f"{sys.argv[3]}")
df_out.to_parquet(f"chunk_{start}_{finish}")