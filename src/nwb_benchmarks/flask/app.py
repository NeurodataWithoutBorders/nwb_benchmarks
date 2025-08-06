import json
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path

import flask
import flask_restx

app = flask.Flask(__name__)
api = flask_restx.Api(
    version="0.1",
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
    @data_namespace.doc(responses={200: "Success", 400: "Bad Request", 500: "Internal server error"})
    def post(self) -> int:
        arguments = contribute_parser.parse_args()
        filename = arguments["filename"]
        test_mode = arguments["test"]

        payload = data_namespace.payload
        json_content = payload["json_content"]

        manager = GitHubResultsManager()
        manager.ensure_repo_exists()
        manager.write_file(filename=filename, json_content=json_content)

        if test_mode is False:
            manager.add_and_commit()
            manager.push()

        return 200


class GitHubResultsManager:
    def __init__(self):
        self.github_token = os.environ.get("GITHUB_API_TOKEN", None)
        if self.github_token is None:
            message = "`GITHUB_API_TOKEN` environment variable is required."
            raise ValueError(message)

        self.repo_url = f"https://{self.github_token}@github.com/codycbakerphd/nwb-benchmarks-results.git"
        self.repo_name = "codycbakerphd/nwb-benchmarks-results"

        self.cache_directory = Path.home() / ".cache" / "nwb-benchmarks"
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        self.repo_path = self.cache_directory / "nwb-benchmarks-results"

    def ensure_repo_exists(self) -> None:
        """Clone repository if it doesn't exist locally."""
        if not self.repo_path.exists():
            command = f"git clone {self.repo_url}"
            cwd = self.cache_directory
        else:
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
            raise RuntimeError(message)

    def write_file(self, filename: str, json_content: dict) -> None:
        """Write results JSON to a file in the cache directory."""
        results_file_path = self.cache_directory / "nwb-benchmarks-results" / filename

        with open(file=results_file_path, mode="w") as file_stream:
            json.dump(obj=json_content, fp=file_stream, indent=4)

    def add_and_commit(self) -> None:
        """Commit results to git repo."""
        command = f"git add . && git commit -m 'Add new benchmark results'"
        result = subprocess.run(
            args=command,
            cwd=self.cache_directory / "nwb-benchmarks-results",
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}"
            raise RuntimeError(message)

    def push(self):
        """Commit and push results to GitHub repository."""
        command = "git push"
        result = subprocess.run(
            args=command,
            cwd=self.cache_directory / "nwb-benchmarks-results",
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
