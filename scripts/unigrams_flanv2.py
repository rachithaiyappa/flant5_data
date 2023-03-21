import pandas as pd
import gzip
import json
import os
from collections import Counter
import pickle as pkl
import sys

file = sys.argv[1]
print(f"working on {file}")
store_to = sys.argv[2]

counts = {}
counts["positive"] = []
counts["negative"] = []
counts["favor"] = []
counts["against"] = []
counts["false"] = []
counts["true"] = []
counts["entails"] = []
counts["contradicts"] = []

counts["Positive"] = []
counts["Negative"] = []
counts["Favor"] = []
counts["Against"] = []
counts["False"] = []
counts["True"] = []
counts["Entails"] = []
counts["Contradicts"] = []

data_input, data_target = [], [] 

with gzip.open(f"/data2/sg/racball/flan_v2/{file}", 'r') as fin: 
    for index,line in enumerate(fin):
        try:
            if index%99999 == 0 and index!= 0:
                print("breaking")
                data_input.extend([json.loads(line)['inputs']])
                data_ = [i.split() for i in data_input]
                data_ = [item for sublist in data_ for item in sublist]
                # data_target.extend([json.loads(line)['targets'] for line in fin])
                d = Counter(data_)

                counts["positive"].append(d["positive"])
                counts["negative"].append(d["negative"])
                counts["favor"].append(d["favor"])
                counts["against"].append(d["against"]) 
                counts["false"].append(d["false"]) 
                counts["true"].append(d["true"]) 
                counts["entails"].append(d["entails"]) 
                counts["contradicts"].append(d["contradicts"]) 


                counts["Positive"].append(d["Positive"])
                counts["Negative"].append(d["Negative"])
                counts["Favor"].append(d["Favor"])
                counts["Against"].append(d["Against"]) 
                counts["False"].append(d["False"]) 
                counts["True"].append(d["True"]) 
                counts["Entails"].append(d["Entails"]) 
                counts["Contradicts"].append(d["Contradicts"]) 

                data_input = []
                continue
            else:
                data_input.extend([json.loads(line)['inputs']])
        except EOFError:
            print("EOF error")
            break


data_ = [i.split() for i in data_input]
data_ = [item for sublist in data_ for item in sublist]
# data_target.extend([json.loads(line)['targets'] for line in fin])
d = Counter(data_)

counts["positive"].append(d["positive"])
counts["negative"].append(d["negative"])
counts["favor"].append(d["favor"])
counts["against"].append(d["against"]) 
counts["false"].append(d["false"]) 
counts["true"].append(d["true"]) 
counts["entails"].append(d["entails"]) 
counts["contradicts"].append(d["contradicts"]) 

counts["Positive"].append(d["Positive"])
counts["Negative"].append(d["Negative"])
counts["Favor"].append(d["Favor"])
counts["Against"].append(d["Against"]) 
counts["False"].append(d["False"]) 
counts["True"].append(d["True"]) 
counts["Entails"].append(d["Entails"]) 
counts["Contradicts"].append(d["Contradicts"]) 


with open(f"{store_to}", "wb") as g:
    pkl.dump(counts,g)     