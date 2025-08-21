import itertools
import json
import pathlib
import time
import warnings

import requests

RESULTS_CACHE_DIR = pathlib.Path.home() / ".cache" / "nwb_benchmarks" / "results"
MACHINES_DIR = pathlib.Path.home() / ".nwb_benchmarks" / "machines"


def clean_results():
    for results_file_path in itertools.chain(
        RESULTS_CACHE_DIR.rglob(pattern="*.json"), MACHINES_DIR.rglob(pattern="*.json")
    ):
        results_file_path.unlink(missing_ok=True)


def upload_results():
    for results_file_path in itertools.chain(
        RESULTS_CACHE_DIR.rglob(pattern="*.json"), MACHINES_DIR.rglob(pattern="*.json")
    ):
        with results_file_path.open("r") as file_stream:
            json_content = json.load(file_stream)

        filename = results_file_path.name
        response = requests.post(
            url=f"https://codycbakerphd.pythonanywhere.com/data/contribute?filename={filename}",
            json={"json_content": json_content},
            timeout=30,
        )
        if response.status_code == 200:
            print(f"Results posted successfully! {filename}")
        else:
            message = (
                "Failed to post results. "
                "Please raise an issue on https://github.com/NeurodataWithoutBorders/nwb_benchmarks/issues."
                f"Status code: {response.status_code} "
                f"Response: {response.text}"
            )
            warnings.warn(message=message, stacklevel=2)

        time.sleep(1)
