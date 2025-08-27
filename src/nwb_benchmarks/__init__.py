"""Outermost exposed imports; including global environment variables."""

import os
import shutil
import warnings

from .command_line_interface import main

# Determine the path for running tshark
TSHARK_PATH = os.environ.get("TSHARK_PATH", None)
NETWORK_INTERFACE = os.environ.get("NWB_BENCHMARKS_NETWORK_INTERFACE", None)

if TSHARK_PATH is None:
    TSHARK_PATH = shutil.which("tshark")

if TSHARK_PATH is None:
    warnings.warn("tshark not found. Set TSHARK_PATH in the environment or install tshark on the default path.")
else:
    if NETWORK_INTERFACE is None:
        warnings.warn("NWB_BENCHMARKS_NETWORK_INTERFACE not found. Set it in the environment.")
    print(f"Using tshark at: {TSHARK_PATH} on {NETWORK_INTERFACE}")

__all__ = [
    "main",
    "TSHARK_PATH",
    "NETWORK_INTERFACE",
]
