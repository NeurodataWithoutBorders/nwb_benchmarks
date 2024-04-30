"""Simple wrapper around `asv run` for convenience."""

import locale
import os
import pathlib
import subprocess
import sys

from .setup import customize_asv_machine_file, reduce_results


def main() -> None:
    """Simple wrapper around `asv run` for convenience."""
    # TODO: swap to click
    if len(sys.argv) <= 1:
        print("No command provided. Please specify a command (e.g. 'run').")
        return

    command = sys.argv[1]
    flags_list = sys.argv[2:]

    debug_mode = "--debug" in flags_list
    bench_mode = "--bench" in flags_list
    if bench_mode:
        specific_benchmark_pattern = flags_list[flags_list.index("--bench") + 1]

    default_asv_machine_file_path = pathlib.Path.home() / ".asv-machine.json"
    if command == "run":
        aws_machine_process = subprocess.Popen(["asv", "machine", "--yes"], stdout=subprocess.PIPE)
        aws_machine_process.wait()
        customize_asv_machine_file(file_path=default_asv_machine_file_path)

        commit_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("ascii").strip()

        # Save latest environment list from conda (most thorough)
        # subprocess tends to have issues inheriting `conda` entrypoint
        asv_root = pathlib.Path(__file__).parent.parent.parent / ".asv"
        asv_root.mkdir(exist_ok=True)

        raw_environment_info_file_path = asv_root / ".raw_environment_info.txt"
        environment_info_process = subprocess.Popen(["conda", "list", ">", raw_environment_info_file_path],
                                         stdout=subprocess.PIPE,
                              shell=True)
        environment_info_process.wait()

        if not raw_environment_info_file_path.exists():
            raise FileNotFoundError(f"Unable to create environment file at {raw_environment_info_file_path}!")

        # Deploy ASV
        cmd = [
            "asv",
            "run",
            "--python=same",
            "--record-samples",
            "--set-commit-hash",
            commit_hash,
        ]
        if debug_mode:
            cmd.extend(["--verbose", "--show-stderr"])
        if bench_mode:
            cmd.extend(["--bench", specific_benchmark_pattern])

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        encoding = locale.getpreferredencoding()  # This is how ASV chooses to encode the output
        for line in iter(process.stdout.readline, b""):
            print(line.decode(encoding).strip("\n"))
        process.stdout.close()
        process.wait()

        globbed_json_file_paths = [
            path
            for path in pathlib.Path(asv_root / "intermediate_results").rglob("*.json")
            if not any(path.stem == skip_stems for skip_stems in ["benchmarks", "machine"])
        ]
        assert (
            len(globbed_json_file_paths) > 0
        ), "No intermediate result was found, likely as a result of a failure in the benchmarks."
        assert len(globbed_json_file_paths) == 1, (
            "A single intermediate result was not found, likely as a result of a previous failure to reduce "
            "the results! Please manually remove these."
        )
        raw_results_file_path = globbed_json_file_paths[0]

        reduce_results(
            raw_results_file_path=raw_results_file_path, raw_environment_info_file_path=raw_environment_info_file_path
        )

    else:
        print(f"{command} is an invalid command.")


if __name__ == "__main__":
    main()
