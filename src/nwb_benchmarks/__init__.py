"""Outermost exposed imports; including global environment variables."""

import os

from .command_line_interface import main

TSHARK_PATH = os.environ.get("TSHARK_PATH", None)
