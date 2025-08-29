import dataclasses
import json
import os
import pathlib
import subprocess
import time
import traceback
import typing
import uuid
from datetime import datetime
from pathlib import Path

import flask
import flask_restx
import packaging.version
import typing_extensions

app = flask.Flask(__name__)
api = flask_restx.Api(
    version="0.3",
    title="upload-nwb-benchmarks-results",
    description="Automatic uploader of NWB Benchmark results.",
)
api.init_app(app)

data_namespace = flask_restx.Namespace(name="data", description="API route for data.")
api.add_namespace(data_namespace)

contribute_parser = flask_restx.reqparse.RequestParser()
contribute_parser.add_argument("filename", type=str, required=True, help="Name of the file to upload.")
contribute_parser.add_argument("test", type=bool, required=False, default=False, help="Test mode flag.")


@data_namespace.route("/contribute")
class Contribute(flask_restx.Resource):
    @data_namespace.expect(contribute_parser)
    @data_namespace.doc(
        responses={
            200: "Success",
            400: "Bad Request",
            500: "Internal server error",
        }
    )
    def post(self) -> int:
        try:
            arguments = contribute_parser.parse_args()
            filename = arguments["filename"]
            test_mode = arguments["test"]

            payload = data_namespace.payload
            json_content = payload["json_content"]

            manager = GitHubResultsManager(repo_name="nwb-benchmarks-results")
            result = manager.ensure_repo_up_to_date()
            if result is not None:
                return result

            time.sleep(1)

            manager.write_file(filename=filename, json_content=json_content)

            time.sleep(1)

            if test_mode is False:
                result = manager.add_and_commit(message="Add new benchmark results")
                if result is not None:
                    return result

                time.sleep(1)

                manager.push()

            return 200
        except Exception as exception:
            return {"type": type(exception).__name__, "error": str(exception), "traceback": traceback.format_exc()}, 500


@data_namespace.route("/update-database")
class UpdateDataBase(flask_restx.Resource):
    @data_namespace.doc(
        responses={
            200: "Success",
            400: "Bad Request",
            500: "Internal server error",
            521: "Repository path on server not found.",
            522: "Error during local update. Check server logs for specifics.",
            523: "Error during add/commit. Check server logs for specifics.",
        }
    )
    def post(self) -> int:
        try:
            directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-results"
            output_directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-database"

            manager = GitHubResultsManager(repo_name="nwb-benchmarks-database")
            result = manager.ensure_repo_up_to_date()
            if result is not None:
                return result

            time.sleep(1)

            repackage_as_parquet(directory=directory, output_directory=output_directory)

            time.sleep(1)

            result = manager.add_and_commit(message="Update benchmark databases")
            if result is not None:
                return result

            time.sleep(1)

            manager.push()

            return 200
        except Exception as exception:
            return {"type": type(exception).__name__, "error": str(exception), "traceback": traceback.format_exc()}, 500


class GitHubResultsManager:
    def __init__(self, repo_name: str):
        self.cache_directory = Path.home() / ".cache" / "nwb-benchmarks"
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        self.repo_path = self.cache_directory / repo_name

    def ensure_repo_up_to_date(self) -> typing.Literal[521, 522] | None:
        """Clone repository if it doesn't exist locally."""
        if not self.repo_path.exists():
            return 521

        command = f"git pull"
        cwd = self.repo_path
        result = subprocess.run(
            args=command,
            cwd=cwd,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}"
            print(f"ERROR: {message}")
            return 522

    def write_file(self, filename: str, json_content: dict) -> None:
        """Write results JSON to a file in the cache directory."""
        base_directory = self.cache_directory / "nwb-benchmarks-results"
        if filename.startswith("environment-"):
            directory = base_directory / "environments"
        elif filename.startswith("machine-"):
            directory = base_directory / "machines"
        elif filename.stem.endswith("_results"):
            directory = base_directory / "results"
        else:
            # Legacy outer collection
            directory = base_directory
        file_path = directory / filename

        with open(file=file_path, mode="w") as file_stream:
            json.dump(obj=json_content, fp=file_stream, indent=4)

    def add_and_commit(self, message: str) -> typing.Literal[523] | None:
        """Commit results to git repo."""
        command = f"git add . && git commit -m '{message}'"
        result = subprocess.run(
            args=command,
            cwd=self.repo_path,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}\ntraceback: {traceback.format_exc()}"
            print(f"ERROR: {message}")
            return 523

    def push(self):
        """Commit and push results to GitHub repository."""
        command = "git push"
        cwd = self.repo_path
        result = subprocess.run(
            args=command,
            cwd=cwd,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}"
            raise RuntimeError(message)


@dataclasses.dataclass
class Machine:
    id: str
    name: str
    version: str
    os: dict
    sys: dict
    platform: dict
    psutil: dict
    cuda: dict
    asv: dict

    @classmethod
    def safe_load_from_json(cls, file_path: pathlib.Path) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(file_stream)

        version = str(data.get("version", None))
        if version is None or packaging.version.Version(version) < packaging.version.Version(version="1.1.0"):
            return None

        machine_id = file_path.stem.removeprefix("machine-")

        return cls(
            id=machine_id,
            name=data.get("name", ""),
            version=version,
            os=data.get("os", {}),
            sys=data.get("sys", {}),
            platform=data.get("platform", {}),
            psutil=data.get("psutil", {}),
            cuda=data.get("cuda", {}),
            asv=data.get("asv", {}),
        )

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "name": self.name,
            "version": self.version,
            "os": json.dumps(self.os),
            "sys": json.dumps(self.sys),
            "platform": json.dumps(self.platform),
            "psutil": json.dumps(self.psutil),
            "cuda": json.dumps(self.cuda),
            "asv": json.dumps(self.asv),
        }

        data_frame = polars.DataFrame(data=data)
        return data_frame


@dataclasses.dataclass
class Environment:
    environment_id: str
    preamble: str

    # Allow arbitrary fields
    def __init__(self, environment_id: str, preamble: str, **kwargs) -> None:
        self.environment_id = environment_id
        self.preamble = preamble
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def safe_load_from_json(cls, file_path: pathlib.Path) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(fp=file_stream)

        if len(data) > 1:
            return None

        environment_id = file_path.stem.removeprefix("environment-")
        preamble = next(iter(data.keys()))

        packages = {
            package["name"]: f"{package["version"]} ({package["build"]})"
            for package in data[preamble]
            if len(package) == 3
        }

        if not any(packages):
            return None

        return cls(environment_id=environment_id, preamble=preamble, **packages)

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "environment_id": self.environment_id,
            "preamble": self.preamble,
        }
        for package_name, package_details in self.__dict__.items():
            if package_name not in ["environment_id", "preamble"]:
                data[package_name] = package_details

        data_frame = polars.DataFrame(data=data, orient="col")
        return data_frame


@dataclasses.dataclass
class Result:
    uuid: str
    version: str
    timestamp: str
    commit_hash: str
    environment_id: str
    machine_id: str
    benchmark_name: str
    parameter_case: str
    value: float


@dataclasses.dataclass
class Results:
    results: list[Result]

    @classmethod
    def safe_load_from_json(cls, file_path: pathlib.Path) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(fp=file_stream)

        database_version = data.get("database_version", None)
        if database_version is None or packaging.version.Version(data["database_version"]) < packaging.version.Version(
            version="1.0.0"
        ):
            return None

        timestamp = data["timestamp"]
        commit_hash = data["commit_hash"]
        environment_id = data["environment_id"]
        machine_id = data["machine_id"]

        results = [
            Result(
                uuid=str(uuid.uuid4()),  # TODO: add this to each results file so it is persistent
                version=database_version,
                timestamp=timestamp,
                commit_hash=commit_hash,
                environment_id=environment_id,
                machine_id=machine_id,
                benchmark_name=benchmark_name,
                parameter_case=parameter_case,
                value=benchmark_result,
            )
            for benchmark_name, parameter_cases in data["results"].items()
            for parameter_case, benchmark_results in parameter_cases.items()
            for benchmark_result in benchmark_results
        ]
        return cls(results=results)

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "uuid": [result.uuid for result in self.results],
            "version": [result.version for result in self.results],
            "commit_hash": [result.commit_hash for result in self.results],
            "environment_id": [result.environment_id for result in self.results],
            "machine_id": [result.machine_id for result in self.results],
            "benchmark_name": [result.benchmark_name for result in self.results],
            "parameter_case": [result.parameter_case for result in self.results],
            "value": [result.value for result in self.results],
        }

        data_frame = polars.DataFrame(data=data)
        return data_frame


def repackage_as_parquet(directory: pathlib.Path, output_directory: pathlib.Path) -> None:
    import polars

    # Machines
    machines_data_frames = []
    machines_directory = directory / "machines"
    for machine_file_path in machines_directory.iterdir():
        machine = Machine.safe_load_from_json(file_path=machine_file_path)

        if machine is None:
            continue

        machine_data_frame = machine.to_dataframe()
        machines_data_frames.append(machine_data_frame)
    machines_database = polars.concat(items=machines_data_frames, how="diagonal_relaxed")

    machines_database_file_path = output_directory / "machines.parquet"
    machines_database.write_parquet(file=machines_database_file_path)

    # Environments
    environments_data_frames = []
    environments_directory = directory / "environments"
    for environment_file_path in environments_directory.iterdir():
        environment = Environment.safe_load_from_json(file_path=environment_file_path)

        if environment is None:
            continue

        environment_data_frame = environment.to_dataframe()
        environments_data_frames.append(environment_data_frame)
    environments_database = polars.concat(items=environments_data_frames, how="diagonal")

    environments_database_file_path = output_directory / "environments.parquet"
    environments_database.write_parquet(file=environments_database_file_path)

    # Results
    all_results_data_frames = []
    results_directory = directory / "results"
    for result_file_path in results_directory.iterdir():
        results = Results.safe_load_from_json(file_path=result_file_path)

        if results is None:
            continue

        results_data_frame = results.to_dataframe()
        all_results_data_frames.append(results_data_frame)
    all_results_database = polars.concat(items=all_results_data_frames, how="diagonal")

    all_results_database_file_path = output_directory / "results.parquet"
    all_results_database.write_parquet(file=all_results_database_file_path)


if __name__ == "__main__":
    DEBUG_MODE = os.environ.get("NWB_BENCHMARKS_DEBUG", None)
    if DEBUG_MODE is not None and DEBUG_MODE != "1":
        message = "NWB_BENCHMARKS_DEBUG environment variable must be set to '1' to run the Flask app in debug mode."
        raise ValueError(message)

    if DEBUG_MODE == "1":
        app.run(debug=True, host="127.0.0.1", port=5000)
    else:
        app.run()
