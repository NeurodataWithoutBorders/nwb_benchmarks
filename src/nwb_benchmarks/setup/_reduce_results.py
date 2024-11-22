"""Default ASV result file is very inefficient - this routine simplifies it for sharing."""

import collections
import datetime
import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import warnings
from typing import Dict, List


def _parse_environment_info(raw_environment_info: List[str]) -> Dict[str, List[[Dict[str, str]]]]:
    """Turn the results of `conda list` printout to a JSON dictionary."""
    header_stripped = raw_environment_info[3:]
    newline_stripped = [line.rstrip("\n") for line in header_stripped]
    # Tried several regex but none quite did the trick in all cases
    spacing_stripped = [[x for x in line.split(" ") if x] for line in newline_stripped]

    keys = ["name", "version", "build", "channel"]
    parsed_environment = {
        sys.version: [{key: value for key, value in zip(keys, values)} for values in spacing_stripped]
    }

    return parsed_environment


def reduce_results(raw_results_file_path: pathlib.Path, raw_environment_info_file_path: pathlib.Path):
    """Default ASV result file is very inefficient - this routine simplifies it for sharing."""
    with open(file=raw_results_file_path, mode="r") as io:
        raw_results_info = json.load(fp=io)
    with open(file=raw_environment_info_file_path, mode="r") as io:
        raw_environment_info = io.readlines()
    parsed_environment_info = _parse_environment_info(raw_environment_info=raw_environment_info)

    # In timestamp, replace separators with underscores for file path
    unix_time_to_datetime = str(datetime.datetime.fromtimestamp(raw_results_info["date"] / 1e3))
    timestamp = unix_time_to_datetime.replace(" ", "-").replace(":", "-")

    environment_hash = hashlib.sha1(string=bytes(json.dumps(obj=parsed_environment_info), "utf-8")).hexdigest()
    machine_hash = raw_results_info["params"]["machine"]

    reduced_results = dict()
    for test_case, raw_results_list in raw_results_info["results"].items():

        # Only successful runs have a results field of length 12
        if len(raw_results_list) != 12:
            continue

        flattened_joint_params = collections.defaultdict(list)
        for parameter_names in raw_results_list[1]:
            for parameter_value_index, parameter_value in enumerate(parameter_names):
                flattened_joint_params[parameter_value_index].append(parameter_value)
        serialized_flattened_joint_params = [
            str(tuple(joint_params)) for joint_params in flattened_joint_params.values()
        ]

        # Skipped results in JSON are writen as `null` and read back into Python as `None`
        non_skipped_results = [result for result in raw_results_list[11] if result is not None]
        if len(serialized_flattened_joint_params) != len(non_skipped_results):
            message = (
                f"In intermediate results for test case {test_case}: \n"
                f"\tLength mismatch between flattened joint parameters ({len(serialized_flattened_joint_params)}) and "
                f"result samples ({len(non_skipped_results)})!\n\n"
                "Please raise an issue and share your intermediate results file."
            )
            warnings.warn(message=message)
            reduced_results.update(
                {
                    test_case: dict(
                        serialized_flattened_joint_params=serialized_flattened_joint_params,
                        non_skipped_results=non_skipped_results,
                        raw_results_list=raw_results_list,
                    )
                }
            )

        else:
            reduced_results.update(
                {
                    test_case: {
                        joint_params: raw_result
                        for joint_params, raw_result in zip(serialized_flattened_joint_params, non_skipped_results)
                    }
                }
            )

    if len(reduced_results) == 0:
        raise ValueError(
            "The results parser failed to find any succesful results! "
            "Please raise an issue and share your intermediate results file."
        )

    reduced_results_info = dict(
        version=raw_results_info["version"],
        timestamp=timestamp,
        commit_hash=raw_results_info["commit_hash"],
        environment_hash=environment_hash,
        machine_hash=machine_hash,
        results=reduced_results,
    )

    # Save reduced results to main .asv folder
    # 'raw' results go to nwb_benchmarks/.asv/intermediate_results/<machine hash>/<results stem>.json
    # 'processed' results go to nwb_benchmarks/results
    main_results_folder = raw_results_file_path.parent.parent.parent.parent / "results"
    parsed_results_file = (
        main_results_folder
        / f"results_timestamp-{timestamp}_machine-{machine_hash}_environment-{environment_hash}.json"
    )
    main_results_folder.mkdir(parents=True, exist_ok=True)

    with open(file=parsed_results_file, mode="w") as io:
        json.dump(obj=reduced_results_info, fp=io, indent=1)  # At least one level of indent makes it easier to read

    # Copy machine file to main results
    machine_info_file_path = raw_results_file_path.parent / "machine.json"
    machine_info_copy_file_path = main_results_folder / f"info_machine-{machine_hash}.json"
    if not machine_info_copy_file_path.exists():
        shutil.copyfile(src=machine_info_file_path, dst=machine_info_copy_file_path)

    # Save parsed environment info within machine subdirectory of .asv
    parsed_environment_file_path = main_results_folder / f"info_environment-{environment_hash}.json"
    if not parsed_environment_file_path.exists():
        with open(file=parsed_environment_file_path, mode="w") as io:
            json.dump(obj=parsed_environment_info, fp=io, indent=1)

    # Network tests require admin permissions, which can alter write permissions of any files created
    if sys.platform in ["darwin", "linux"]:
        subprocess.run(["chmod", "-R", "+rw", parsed_results_file.absolute()])
        subprocess.run(["chmod", "-R", "+rw", machine_info_file_path.absolute()])
        subprocess.run(["chmod", "-R", "+rw", parsed_environment_file_path.absolute()])

    raw_results_file_path.unlink()
