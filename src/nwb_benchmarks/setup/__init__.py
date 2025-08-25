"""Exposed imports to the `setup` submodule."""

from ._configure_machine import (
    collect_machine_info,
    generate_human_readable_machine_name,
    generate_machine_file,
)
from ._reduce_results import reduce_results

__all__ = [
    "collect_machine_info",
    "generate_machine_file",
    "generate_human_readable_machine_name",
    "reduce_results",
]
