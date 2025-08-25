import fsspec


def download_file(s3_url: str, local_path: str):
    """
    Download a file from S3 URL to a local path.

    Parameters
    ----------
    s3_url : str
        The S3 URL of the file to download.
    local_path : str
        The local path where the file should be saved.
    """

    print(f"Downloading {s3_url} to {local_path}...", end="")
    with fsspec.open(s3_url, "rb") as src:
        with open(local_path, "wb") as dst:
            dst.write(src.read())
    print(" done.")
