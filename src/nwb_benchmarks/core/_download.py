import fsspec


def download_file(url: str, local_path: str):
    """
    Download a file from a URL to a local path.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    local_path : str
        The local path where the file should be saved.
    """

    print(f"Downloading {url} to {local_path}...", end="")
    with fsspec.open(url, "rb") as src:
        with open(local_path, "wb") as dst:
            dst.write(src.read())
    print(" done.")
