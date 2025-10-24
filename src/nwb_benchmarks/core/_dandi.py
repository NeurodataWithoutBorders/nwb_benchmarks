from dandi.dandiapi import DandiAPIClient


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
