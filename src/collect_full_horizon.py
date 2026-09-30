#!/usr/bin/env python3
"""PREP09 compatibility adapter; removal requires the layout contract exit gate."""
import importlib as _importlib
import sys as _sys

_implementation = _importlib.import_module("mardorf_collector.runtime.collect_full_horizon")
if __name__ == "__main__":
    _sys.exit(_implementation.main())
else:
    _sys.modules[__name__] = _implementation
