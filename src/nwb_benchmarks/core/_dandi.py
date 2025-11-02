import posixpath

from dandi.dandiapi import DandiAPIClient
from dandi.download import parse_dandi_url, download, DownloadExisting

from ..setup import get_persistent_download_directory


def get_https_url(dandiset_id: str, dandi_path: str, follow_redirects: bool | int = 1) -> str:
    """
    Helper function to get S3 url form that fsspec/remfile expect from basic info about a file on DANDI.

    Parameters
    ----------
    dandiset_id : string
        Six-digit identifier of the dandiset.
        For the NWB Benchmark project, the primary ones are 000717 (HDF5) and 000719 (Zarr).
    dandi_path : string
        The relative path of the file to the dandiset.
        For example, "sub-".
    """
    assert len(dandiset_id) == 6, f"The specified 'dandiset_id' ({dandiset_id}) should be the six-digit identifier."

    if int(dandiset_id) >= 200_000:
        api_url = "https://api-staging.dandiarchive.org/api"
    else:
        api_url = "https://api.dandiarchive.org/api"

    client = DandiAPIClient(api_url=api_url)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)
    asset = dandiset.get_asset_by_path(path=dandi_path)

    https_url = asset.get_content_url(follow_redirects=follow_redirects, strip_query=True)
    return https_url


def get_asset_path_from_url(https_url: str) -> str:
    """
    Given a DANDI HTTPS URL, return the basename of the asset path within the dandiset.
    This is the filename of the asset if one were to call dandi.download on the URL.

    Parameters
    ----------
    https_url : str
        The HTTPS URL of the asset on DANDI.

    Returns
    -------
    str
        The basename of the asset path within the dandiset.
    """
    dandi_url = parse_dandi_url(https_url)
    asset = list(dandi_url.get_assets(dandi_url.get_client()))[0]
    return posixpath.basename(asset.path)


def download_asset_if_not_exists(https_url: str) -> str:
    """
    Download the asset from the given DANDI HTTPS URL if it does not already exist in the persistent download directory.

    NOTE: Getting the asset path from the URL can take a little time so this function should not be included in the
    timing or network tracking of benchmarks.

    Parameters
    ----------
    https_url : str
        The HTTPS URL of the asset on DANDI.

    Returns
    -------
    str
        The file path of the downloaded asset.
    """
    download_dir = get_persistent_download_directory()
    download(urls=https_url, output_dir=download_dir, existing=DownloadExisting.OVERWRITE_DIFFERENT)
    filename = get_asset_path_from_url(https_url=https_url)
    return str(download_dir / filename)