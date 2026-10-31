#!/usr/bin/env python3
"""herdr plugin entry point; the implementation lives in cmux_herdr_core."""

import sys

from cmux_herdr_core.cli import main

if __name__ == "__main__":
    sys.exit(main())
