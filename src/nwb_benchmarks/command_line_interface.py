"""Simple wrapper around `asv run` for conveneience."""
import sys
import subprocess
import locale


def main():
    """Simple wrapper around `asv run` for conveneience."""
    # TODO: swap to click
    if len(sys.argv) <= 1:
        print("No command provided. Please specify a command (e.g. 'run').")
        return

    command = sys.argv[1]

    if command == "run":
        commit_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("ascii").strip()

        cmd = [
            "asv",
            "run",
            "--python=same",
            "--record-samples",
            "--set-commit-hash",
            commit_hash,
        ]

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
