"""Simple wrapper around `asv run` for convenience."""

import locale
import pathlib
import subprocess
import sys

from .setup import customize_asv_machine_file, ensure_machine_info_current


def main():
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

    if command == "run":
        commit_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("ascii").strip()

        default_asv_machine_file_path = pathlib.Path.home() / ".asv-machine.json"
        if default_asv_machine_file_path.exists():
            ensure_machine_info_current(file_path=default_asv_machine_file_path)
        else:
            customize_asv_machine_file(file_path=default_asv_machine_file_path)

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

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)  # , bufsize=1)
        encoding = locale.getpreferredencoding()  # This is how ASV chooses to encode the output
        for line in iter(process.stdout.readline, b""):
            print(line.decode(encoding).strip("\n"))
        process.stdout.close()
        process.wait()
    else:
        print(f"{command} is an invalid command.")


if __name__ == "__main__":
    main()
