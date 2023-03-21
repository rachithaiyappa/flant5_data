import os
import numpy as np


total = 1024
chunk_size = 50
STARTS = np.arange(0,total, chunk_size)
FINISHES = [s+chunk_size for s in STARTS]

STARTS = [1000]
FINISHES = [1024]
OUTPUTS = os.path.join("data", "chunk_{start}_{finish}.parquet")

rule all:
    input:
        expand(OUTPUTS,zip,start = STARTS, finish=FINISHES)

rule intersection:
    input:
        script = "scripts/text_counter.py"
    output:
        data = OUTPUTS
    shell:
        "python3 {input.script} {wildcards.start} {wildcards.finish} {output.data}"