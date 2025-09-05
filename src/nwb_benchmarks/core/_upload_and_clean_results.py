import itertools
import json
import pathlib
import time
import warnings

import requests

from ..globals import (
    ENVIRONMENTS_DIR,
    LOGS_DIR,
    MACHINES_DIR,
    RESULTS_DIR,
)
from ..setup import get_benchmarks_home_directory


def clean_results():
    upload_tracker_file_path = get_benchmarks_home_directory() / "upload_tracker.json"
    upload_tracker_file_path.unlink(missing_ok=True)

    for results_file_path in itertools.chain(
        RESULTS_DIR.rglob(pattern="*.json"),
        MACHINES_DIR.rglob(pattern="*.json"),
        ENVIRONMENTS_DIR.rglob(pattern="*.json"),
        LOGS_DIR.rglob(pattern="*.txt"),
    ):
        results_file_path.unlink(missing_ok=True)


def upload_results():
    upload_tracker_file_path = get_benchmarks_home_directory() / "upload_tracker.json"
    if upload_tracker_file_path.exists() is False:
        upload_tracker_file_path.write_text(data="{}")

    with upload_tracker_file_path.open(mode="r") as file_stream:
        upload_tracker = json.load(fp=file_stream)

    try:
        all_file_paths_to_upload = list(
            itertools.chain(
                RESULTS_DIR.rglob(pattern="*.json"),
                MACHINES_DIR.rglob(pattern="*.json"),
                ENVIRONMENTS_DIR.rglob(pattern="*.json"),
            )
        )
        file_paths_to_upload = [
            file_path for file_path in all_file_paths_to_upload if upload_tracker.get(file_path.name, False) is False
        ]

        for file_path in file_paths_to_upload:
            with file_path.open("r") as file_stream:
                json_content = json.load(file_stream)

            filename = file_path.name
            response = requests.post(
                url=f"https://codycbakerphd.pythonanywhere.com/data/contribute?filename={filename}",
                json={"json_content": json_content},
                timeout=30,
            )
            if response.status_code == 200:
                upload_tracker[filename] = True
                print(f"Results posted successfully! {filename}")
            else:
                message = (
                    f"Failed to post results. {filename} "
                    "Please raise an issue on https://github.com/NeurodataWithoutBorders/nwb_benchmarks/issues. "
                    f"Status code: {response.status_code} "
                    f"Response: {response.text}"
                )
                warnings.warn(message=message, stacklevel=2)

            time.sleep(1)
    finally:
        with upload_tracker_file_path.open(mode="w") as file_stream:
            json.dump(obj=upload_tracker, fp=file_stream, indent=1)
