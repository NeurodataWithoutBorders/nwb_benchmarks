"""Add information to the ASV machine parameters."""

import copy
import datetime
import hashlib
import json
import os
import pathlib
import platform
import sys
from typing import Any, Dict

import friendlywords
import psutil
from numba import cuda

from ..globals import MACHINE_FILE_VERSION, MACHINES_DIR
from ..utils import get_dictionary_checksum


def collect_machine_info() -> Dict[str, Dict[str, Any]]:
    """Collect attributes for uniquely identifying a system and providing metadata associated with performance."""
    machine_info = dict()

    is_github_runner = os.getenv("GITHUB_ACTIONS", False)
    timestamp = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S")
    machine_name = f"github-{timestamp}" if is_github_runner else generate_human_readable_machine_name()
    machine_info["name"] = machine_name
    machine_info["version"] = MACHINE_FILE_VERSION

    machine_info["os"] = dict(cpu_count=os.cpu_count())
    machine_info["sys"] = dict(platform=sys.platform)
    machine_info["platform"] = dict(
        architecture=list(platform.architecture()),  # Must be cast as a list for later assertions against JSON
        machine=platform.machine(),
        platform=platform.platform(),
        processor=platform.processor(),
        system=platform.system(),
    )
    machine_info["psutil"] = dict(
        number_of_processes=psutil.cpu_count(logical=False),
        number_of_threads=psutil.cpu_count(logical=True),
        total_virtual_memory=psutil.virtual_memory().total,
        total_swap_memory=psutil.swap_memory().total,
        disk_partitions=[disk_partition._asdict() for disk_partition in psutil.disk_partitions()],
    )
    # TODO: psutil does have some socket stuff in .net_connections, is that useful at all?

    # GPU info - mostly taken from https://stackoverflow.com/a/62459332
    machine_info["cuda"] = dict()
    try:
        device = cuda.get_current_device()
        gpu_attributes = [
            name.replace("CU_DEVICE_ATTRIBUTE_", "")
            for name in dir(cuda.cudadrv.enums)
            if name.startswith("CU_DEVICE_ATTRIBUTE_")
        ]
        gpu_specifications = {gpu_attribute: getattr(device, gpu_attribute) for gpu_attribute in gpu_attributes}
        machine_info["cuda"] = dict(gpu_name=device.name.decode("utf-8"), gpu_specifications=gpu_specifications)
    except cuda.cudadrv.error.CudaSupportError:
        # No GPU detected by cuda; skipping section of custom machine info
        pass
    except Exception as exception:
        raise exception

    default_asv_machine_file_path = pathlib.Path.home() / ".asv-machine.json"
    if default_asv_machine_file_path.exists():
        with open(file=default_asv_machine_file_path, mode="r") as file_stream:
            asv_machine_info = json.load(fp=file_stream)
    machine_info["asv"] = asv_machine_info

    # Some info in ASV may be considered 'private'
    if len(asv_machine_info.keys()) != 2:
        message = (
            f"\nThe ASV machine file at {default_asv_machine_file_path} should only contain two keys: "
            "'version' and the machine name. "
            f"Found {len(asv_machine_info.keys())} keys: {list(asv_machine_info.keys())}' "
            "Please raise an issue at https://github.com/NeurodataWithoutBorders/nwb_benchmarks/issues/new to report."
        )
        raise ValueError(message)

    asv_machine_key = next(key for key in asv_machine_info.keys() if key != "version")
    asv_machine_info[asv_machine_key]["machine"] = machine_name

    anonymized_asv_machine_info = {
        machine_name: asv_machine_info[asv_machine_key],
        "version": asv_machine_info["version"],
    }
    machine_info["asv"] = anonymized_asv_machine_info

    return machine_info


def get_machine_file_checksum(machine_info: dict) -> str:
    """Get the checksum of the machine info after removing the machine name."""
    stable_machine_info = copy.deepcopy(x=machine_info)
    stable_machine_info["name"] = ""
    asv_machine_key = next(key for key in stable_machine_info["asv"].keys() if key != "version")
    stable_machine_info["asv"] = {"": machine_info["asv"][asv_machine_key]}
    stable_machine_info["asv"][""].pop("machine")
    checksum = get_dictionary_checksum(dictionary=stable_machine_info)

    return checksum


def generate_machine_file() -> str:
    """Generate a custom machine file and store in the NWB Benchmarks home directory."""
    machine_info = collect_machine_info()

    checksum = get_machine_file_checksum(machine_info=machine_info)

    machine_info_file_path = MACHINES_DIR / f"machine-{checksum}.json"
    if machine_info_file_path.exists():
        print(f"\nMachine info exists at:    {machine_info_file_path}\n")
        return

    with open(file=machine_info_file_path, mode="w") as file_stream:
        json.dump(obj=machine_info, fp=file_stream, indent=1)
    print(f"\nMachine info written to:    {machine_info_file_path}\n")

    return checksum


def generate_human_readable_machine_name() -> str:
    """
    There are about 4 million possible combinations of predicate/objects in `friendlywords`.

    GitHub Actions runners get unique names based on the timestamp of the run.
    Natural collisions should be able to be disambiguated by the peripheral machine info.
    """
    name = "".join(word.capitalize() for word in friendlywords.generate(command="po", as_list=True))
    return name
