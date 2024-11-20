"""Outermost exposed imports; including global environment variables."""

import os
import warnings

from .command_line_interface import main

TSHARK_PATH = os.environ.get("TSHARK_PATH", None)

if TSHARK_PATH is None:
    msg = "TSHARK_PATH is not set in the environment."
    warnings.warn(msg)
