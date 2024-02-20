"""Add information to the ASV machine parameters."""

import hashlib
import json
import os
import pathlib
import platform
import sys
import warnings
from typing import Any, Dict

import psutil
from numba import cuda


def collect_machine_info() -> Dict[str, Dict[str, Any]]:
    """Collect attributes for uniquely identifying a system and providing metadata associated with performance."""
    custom_machine_info = dict()

    custom_machine_info["os"] = dict(cpu_count=os.cpu_count())
    custom_machine_info["sys"] = dict(platform=sys.platform)
    custom_machine_info["platform"] = dict(
        architecture=list(platform.architecture()),  # Must be cast as a list for later assertions against JSON
        machine=platform.machine(),
        platform=platform.platform(),
        processor=platform.processor(),
        system=platform.system(),
    )
    custom_machine_info["psutil"] = dict(
        number_of_processes=psutil.cpu_count(logical=False),
        number_of_threads=psutil.cpu_count(logical=True),
        total_virtual_memory=psutil.virtual_memory().total,
        total_swap_memory=psutil.swap_memory().total,
        disk_partitions=[disk_partition._asdict() for disk_partition in psutil.disk_partitions()],
    )
    # TODO: psutil does have some socket stuff in .net_connections, is that useful at all?

    # GPU info - mostly taken from https://stackoverflow.com/a/62459332
    try:
        device = cuda.get_current_device()
        gpu_attributes = [
            name.replace("CU_DEVICE_ATTRIBUTE_", "")
            for name in dir(cuda.cudadrv.enums)
            if name.startswith("CU_DEVICE_ATTRIBUTE_")
        ]
        gpu_specifications = {gpu_attribute: getattr(device, gpu_attribute) for gpu_attribute in gpu_attributes}
        custom_machine_info["cuda"] = dict(gpu_name=device.name.decode("utf-8"), gpu_specifications=gpu_specifications)
    except cuda.cudadrv.error.CudaSupportError:
        # No GPU detected by cuda; skipping section of custom machine info
        pass
    except Exception as exception:
        raise exception

    return custom_machine_info


def customize_asv_machine_file(file_path: pathlib.Path, overwrite: bool = False) -> None:
    """Modify ASV machine file in-place with additional values."""
    with open(file=file_path, mode="r") as io:
        machine_file_info = json.load(fp=io)

    # Assume there's only one machine configured per installation
    default_machine_name = next(key for key in machine_file_info.keys() if key != "version")

    # Add flag for detecting if this modification script has been run on the file already
    if not overwrite and machine_file_info[default_machine_name].get("custom", False):
        return

    custom_machine_info = collect_machine_info()

    if overwrite and "defaults" in machine_file_info[default_machine_name]:
        default_machine_info = machine_file_info[default_machine_name]["defaults"]
    else:
        default_machine_info = machine_file_info[default_machine_name]
    custom_machine_info["defaults"] = default_machine_info  # Defaults tends to have blanks

    # Required keys at the outer level
    # The 'machine' key is really the 'ID' of the machines logs
    # Needs to be unique for a combined results database
    custom_machine_info.update(custom=True)
    custom_machine_hash = hashlib.sha1(
        string=bytes(json.dumps(obj=custom_machine_info, sort_keys=True), "utf-8")
    ).hexdigest()
    custom_machine_info.update(machine=custom_machine_hash)

    custom_file_info = {
        custom_machine_hash: custom_machine_info,
        "version": machine_file_info["version"],
    }

    with open(file=file_path, mode="w") as io:
        json.dump(fp=io, obj=custom_file_info, indent=4)


def ensure_machine_info_current(file_path: pathlib.Path):
    """
    Even something as simple as adjusting the disk partitions could affect performance.

    If out of date, automatically trigger regeneration of machine info and hash.
    Will also likely be affected by ipcfg.
    """
    current_machine_info = collect_machine_info()

    # Assume there's only one machine configured per installation
    with open(file=file_path, mode="r") as io:
        machine_file = json.load(fp=io)

    default_machine_name = next(key for key in machine_file.keys() if key != "version")
    machine_info_from_file = machine_file[default_machine_name]

    # Only asserting agains the custom stuff we grab
    machine_info_from_file.pop("defaults")
    machine_info_from_file.pop("machine")
    machine_info_from_file.pop("custom")

    if machine_info_from_file == current_machine_info:
        return

    # If debugging is ever necessary in the future, best way I found to summarize differences was
    # import unittest
    #
    # test = unittest.TestCase()
    # test.maxDiff = None
    # test.assertDictEqual(d1=machine_info_from_file, d2=current_machine_info)

    warnings.warn("The current machine info is out of date! Automatically updating the file.", stacklevel=2)
    customize_asv_machine_file(file_path=file_path, overwrite=True)
