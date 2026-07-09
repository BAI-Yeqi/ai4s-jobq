#!/usr/bin/env python3
from ai4s.jobq.workflow import set_output

print("Featurizing...")
set_output({"path": "/tmp/features.parquet", "rows": 50000})
print("Done.")
