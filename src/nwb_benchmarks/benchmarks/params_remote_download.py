from nwb_benchmarks.core import get_https_url

# similar to params_remote_file_reading.py but with follow_redirects=False since download does not work with redirected url

hdf5_params = (
    dict(
        name="EcephysTestCase",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
            follow_redirects=False,
        ),
    ),
    dict(
        name="OphysTestCase",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
            follow_redirects=False,
        ),
    ),
    dict(
        name="IcephysTestCase",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
            follow_redirects=False,
        ),
    ),
)

zarr_params = (
    dict(
        name="EcephysTestCase",
        https_url=get_https_url(
            dandiset_id="000719",
            dandi_path="sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr",
            follow_redirects=False,
        ),
    ),
    dict(
        name="OphysTestCase",
        https_url=get_https_url(
            dandiset_id="000719",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr",
            follow_redirects=False,
        ),
    ),
    dict(
        name="IcephysTestCase",
        https_url=get_https_url(
            dandiset_id="000719",
            dandi_path="icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr",
            follow_redirects=False,
        ),
    ),
)

lindi_remote_rfs_params = (
    dict(
        name="EcephysTestCase",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
    ),
    dict(
        name="OphysTestCase",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
    ),
    dict(
        name="IcephysTestCase",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
    ),
)
