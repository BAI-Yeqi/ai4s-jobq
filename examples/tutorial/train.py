#!/usr/bin/env python3
from ai4s.jobq.workflow import get_upstream_output, set_output

features = get_upstream_output("featurize")
print(f"Training on {features['path']} ({features['rows']} rows)")

# ... training logic ...

set_output({"model": "/tmp/model.pt", "mae": 0.03})
print("Training complete.")
