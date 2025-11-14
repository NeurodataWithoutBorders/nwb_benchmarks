"""
Extract dependencies from pyproject.toml, resolve versions at a specific date,
and creates a new environment.yaml with version constraints.
Use for testing historical versions of packages before benchmarks results
database and automated publishing was finalized.
"""

import subprocess
import tempfile


def create_older_environment_yaml(
    packages_to_restrict: tuple[str],
    exclude_newer_date: str = "2025-06-30",
    output_file_base: str = "nwb_benchmarks",
):
    # create temporary input requirements file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt") as tmp_file:
        tmp_file.write("\n".join(packages_to_restrict))
        tmp_file.flush()
        tmp_path = tmp_file.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt") as tmp_out:
            tmp_out_path = tmp_out.name

            # run uv pip compile to get versions from a specific date
            cmd = ["uv", "pip", "compile", tmp_path, "--exclude-newer", exclude_newer_date, "-o", tmp_out_path]
            subprocess.run(cmd)

            # parse the output to get pinned versions based on date
            pinned_requirements = {}
            with open(tmp_out_path, "r") as f:
                for line in f:
                    if "==" in line:
                        pkg_name = line.split("==")[0].strip()
                        version = line.split("==")[1].split()[0].strip()
                        pinned_requirements[pkg_name] = version

            # save to new output file
            # NOTE: will create very minimal environment file, rest of dependencies will be installed with -e ..
            with open(f"environments/{output_file_base}_{exclude_newer_date}.yaml", "w") as f:
                f.write(
                    f"name: nwb_benchmarks_{exclude_newer_date}\n"
                    f"dependencies:\n"
                    f"  - python == 3.11\n"  # use for consistency
                    f"  - git\n"
                    f"  - conda-forge::h5py <={pinned_requirements['h5py']}\n"
                    f"  - pip\n"
                    f"  - pip:\n"
                )
                for pkg, version in sorted(pinned_requirements.items()):
                    if pkg in packages_to_restrict and pkg not in ["h5py"]:
                        f.write(f"    - {pkg}<={version}\n")
                f.write(f"    - -e ..\n")


if __name__ == "__main__":
    # restrict key packages of interest to specific dates
    # no versions of lindi pre 2024-03 and other more difficult dependency resolution issues, so stop at 2024-06-30
    packages_to_restrict = ["h5py", "dandi", "zarr", "fsspec", "lindi", "s3fs", "aiohttp", "boto3"]
    create_older_environment_yaml(exclude_newer_date="2025-06-30", packages_to_restrict=packages_to_restrict)
    create_older_environment_yaml(exclude_newer_date="2024-06-30", packages_to_restrict=packages_to_restrict)
