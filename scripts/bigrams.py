import gzip
import json
import pickle as pkl
from collections import Counter 
import re
from itertools import islice
import sys

global_bigrams = {}

inputs = sys.argv[1]
start = int(inputs.split('_')[1])
finish = int(inputs.split('_')[2].split('--')[0])
if finish > 1024:
    finish = 1024
target = str(inputs.split('_')[2].split('--')[1].split('.')[0])

# start = int(sys.argv[1])
# finish = int(sys.argv[2])
# target = str(sys.argv[3])

print(f"{start},{finish},{target}")

for num in range(start,finish):

    label = str(num).zfill(5)
    jsonfilename = f'/data2/sg/racball/c4/en/c4-train.{label}-of-01024.json.gz'

    with gzip.open(jsonfilename, 'r') as fin:
        data = [json.loads(line)['text'] for line in fin]

    counts = []
    for k,sentence in enumerate(data): 
        words = re.findall("\w+", sentence)
        local_dict = dict(Counter(zip(words, islice(words, 1, None))))

        count = [local_dict[i] for i in local_dict.keys() if i[0] == f"{target}" or i[1] == f"{target}"]
        for i in local_dict.keys():
            if i[0] == f"{target}" or i[1] == f"{target}" :
                if i in global_bigrams:
                    global_bigrams[i] += sum(count)
                else:
                    global_bigrams[i] = sum(count)

with open(f"{inputs}","wb") as f:
    pkl.dump(global_bigrams,f)