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
        architecture=platform.architecture(),
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
        disk_partitions=psutil.disk_partitions(),
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
        warnings.warn("No GPU detected by cuda; skipping section of custom machine info.")
    except Exception as exception:
        raise exception


def customize_asv_machine_file(file_path: pathlib.Path) -> None:
    """Modify ASV machine file in-place with additional values."""
    with open(file=default_asv_machine_file_path, mode="r") as io:
        machine_file_info = json.load(fp=io)

    # Assume there's only one machine configured per installation
    default_machine_name = next(key for key in machine_file_info.keys() if key != "version")

    # Add flag for detecting if this modification script has been run on the file already
    if machine_file_info[default_machine_name].get("custom", False):
        return

    default_machine_info = machine_file_info[default_machine_name]
    custom_machine_info = collect_machine_info()

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

    with open(file=default_asv_machine_file_path, mode="w") as io:
        json.dump(fp=io, obj=custom_file_info, indent=4)


if __name__ == "__main__":
    home_directory = pathlib.Path.home()

    # This should theoretically always be the location across systems
    default_asv_machine_file_path = home_directory / ".asv-machine.json"

    customize_asv_machine_file(file_path=default_asv_machine_file_path)
