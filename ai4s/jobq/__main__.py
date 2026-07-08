# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Allow ``python -m ai4s.jobq`` to invoke the CLI from the dev tree."""

from __future__ import annotations

import sys

from ai4s.jobq.cli import main

if __name__ == "__main__":
    sys.exit(main())
