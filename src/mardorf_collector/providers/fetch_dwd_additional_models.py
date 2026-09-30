"""Package bridge to byte-frozen provider source; no duplicate implementation."""
import importlib as _importlib
import sys as _sys

_implementation = _importlib.import_module("fetch_dwd_additional_models")
_sys.modules[__name__] = _implementation
