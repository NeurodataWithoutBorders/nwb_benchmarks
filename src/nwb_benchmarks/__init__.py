"""Outermost exposed imports; including global environment variables."""

import os
import shutil
import warnings

from .command_line_interface import main

# Determine the path for running tshark
TSHARK_PATH = os.environ.get("TSHARK_PATH", None)
if TSHARK_PATH is None:
    TSHARK_PATH = shutil.which("tshark")

if TSHARK_PATH is None:
    warnings.warn("tshark not found. Set TSHARK_PATH in the environment or install tshark on the default path.")
else:
    print(f"Using tshark at: {TSHARK_PATH}")

__all__ = [
    "main",
    "TSHARK_PATH",
]
