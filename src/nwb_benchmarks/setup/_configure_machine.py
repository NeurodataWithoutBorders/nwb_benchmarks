"""Add information to the ASV machine parameters."""

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

MACHINE_FILE_VERSION = "1.0.0"


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
            asv_machine_file_info = json.load(fp=file_stream)
    machine_info["asv"] = asv_machine_file_info

    return machine_info


def generate_machine_file() -> str:
    """Generate a custom machine file and store in the NWB Benchmarks home directory."""
    machine_info = collect_machine_info()

    nwb_benchmarks_home_directory = pathlib.Path.home() / ".nwb_benchmarks"
    nwb_benchmarks_home_directory.mkdir(exist_ok=True)
    machines_directory = nwb_benchmarks_home_directory / "machines"
    machines_directory.mkdir(exist_ok=True)

    checksum = get_machine_info_checksum(info=machine_info)
    machine_info_file_path = machines_directory / f"{checksum}.json"
    with open(file=machine_info_file_path, mode="w") as file_stream:
        json.dump(obj=machine_info, fp=file_stream, indent=1)

    return checksum


def generate_human_readable_machine_name() -> str:
    """
    There are about 4 million possible combinations of predicate/objects in `friendlywords`.

    GitHub Actions runners get unique names based on the timestamp of the run.
    Natural collisions should be able to be disambiguated by the peripheral machine info.
    """
    name = "".join(word.capitalize() for word in friendlywords.generate(command="po", as_list=True))
    return name


def get_machine_info_checksum(info: dict) -> str:
    """Get the SHA1 hash of the machine info."""
    sorted_info = dict(sorted(info.items()))
    checksum = hashlib.sha1(string=bytes(json.dumps(obj=sorted_info))).hexdigest()
    return checksum
