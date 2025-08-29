from nwb_benchmarks.core import get_https_url

parameter_cases = dict(
    EcephysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
    # EcephysTestCase=dict(
    #     https_url=get_https_url(
    #         dandiset_id="000717",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    #     ),
    #     object_name="ElectricalSeriesAp",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # ),
)

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
    # EcephysTestCase=dict(
    #     https_url=get_https_url(
    #         dandiset_id="213889",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.lindi.json",
    #     ),
    #     object_name="ElectricalSeriesAp",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # ),
)

# TODO Test non-consolidated metadata vs consolidated metadata
zarr_parameter_cases = dict(
    EcephysTestCase=dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
    # AINDTestCase=dict(
    #     https_url=(
    #         "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
    #         "ecephys_625749_2022-08-03_15-15-06_nwb/"
    #         "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
    #     ),
    #     object_name="ElectricalSeriesProbe A-LFP",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # )
)
