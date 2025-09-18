import json
import os
import pathlib
import subprocess
import time
import traceback
import typing
from pathlib import Path

import flask
import flask_restx

from nwb_benchmarks.database._processing import repackage_as_parquet

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
            filename = pathlib.Path(arguments["filename"])
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

            repackage_as_parquet(directory=directory, output_directory=output_directory, minimum_version="3.0.0")

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
        self.debug_mode = os.environ.get("NWB_BENCHMARKS_DEBUG") == "1"

    def ensure_repo_up_to_date(self) -> typing.Literal[521, 522] | None:
        """Pull latest updates from repository. Clone repository first if it doesn't exist locally."""
        if not self.repo_path.exists():
            command = f"git clone https://github.com/NeurodataWithoutBorders/nwb-benchmarks-database"
            cwd = self.repo_path.parent
            cwd.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(
                args=command,
                cwd=cwd,
                capture_output=True,
                shell=True,
            )
            if result.returncode != 0:
                message = f"Git command ({command}) failed: {traceback.format_exc()}"
                print(f"ERROR: {message}")
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

    def write_file(self, filename: pathlib.Path, json_content: dict) -> None:
        """Write results JSON to a file in the cache directory."""
        base_directory = self.cache_directory / "nwb-benchmarks-results"
        filestem: str = filename.stem
        if filestem.startswith("environment-"):
            directory = base_directory / "environments"
        elif filestem.startswith("machine-"):
            directory = base_directory / "machines"
        elif filestem.endswith("_results"):
            directory = base_directory / "results"
        else:
            # Legacy outer collection
            directory = base_directory
        file_path = directory / filename

        with open(file=file_path, mode="w") as file_stream:
            json.dump(obj=json_content, fp=file_stream, indent=4)

    def add_and_commit(self, message: str) -> typing.Literal[523] | None:
        """Commit results to git repo."""
        if self.debug_mode:
            print("Debug mode enabled, skipping git commit.")
            return None

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
        if self.debug_mode:
            print("Debug mode enabled, skipping git push.")
            return None

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


if __name__ == "__main__":
    DEBUG_MODE = os.environ.get("NWB_BENCHMARKS_DEBUG", None)
    if DEBUG_MODE is not None and DEBUG_MODE != "1":
        message = "NWB_BENCHMARKS_DEBUG environment variable must be set to '1' to run the Flask app in debug mode."
        raise ValueError(message)

    if DEBUG_MODE == "1":
        app.run(debug=True, host="127.0.0.1", port=5000)
    else:
        app.run()
