import json
import pathlib
import time
import warnings

import requests


def upload_results():
    results_cache_directory = pathlib.Path.home() / ".cache" / "nwb_benchmarks" / "results"
    for results_file_path in results_cache_directory.rglob(pattern="*.json"):
        with results_file_path.open("r") as file_stream:
            json_content = json.load(file_stream)

        filename = results_file_path.name
        response = requests.post(
            url=f"https://codycbakerphd.pythonanywhere.com/data/contribute?filename={filename}",
            json={"json_content": json_content},
            timeout=30,
        )
        if response.status_code == 200:
            print(f"Results posted successfully!")
        else:
            message = (
                "Failed to post results. "
                "Please raise an issue on https://github.com/NeurodataWithoutBorders/nwb_benchmarks/issues."
                f"Status code: {response.status_code} "
                f"Response: {response.text}"
            )
            warnings.warn(message=message, stacklevel=2)

        time.sleep(1)
