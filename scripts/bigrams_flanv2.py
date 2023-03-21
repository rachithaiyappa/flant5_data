import gzip
import json
import pickle as pkl
from collections import Counter 
import re
from itertools import islice
import sys



inputs = sys.argv[1]
print(inputs)
file = str(inputs.split('/')[-1].split('__')[0])
target = str(inputs.split('/')[-1].split('--')[1].split('.')[0])


print(f"{file},{target}")

global_bigrams = {}
with gzip.open(f"/data2/sg/racball/flan_v2/{file}", 'r') as fin: 
    for index,line in enumerate(fin):
        counts = []
        words = re.findall("\w+", json.loads(line)['inputs'])
        local_dict = dict(Counter(zip(words, islice(words, 1, None))))
        count = [local_dict[i] for i in local_dict.keys() if i[0] == f"{target}" or i[1] == f"{target}"]
        for i in local_dict.keys():
            if i[0] == f"{target}" or i[1] == f"{target}" :
                if i in global_bigrams:
                    global_bigrams[i] += sum(count)
                else:
                    global_bigrams[i] = sum(count)



with open(f"{inputs}", "wb") as g:
    pkl.dump(global_bigrams,g)