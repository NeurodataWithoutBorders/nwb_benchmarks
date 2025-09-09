from nwb_benchmarks.core import get_https_url

hdf5_params = (
    dict(
        name="EcephysTestCase1",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144), slice(0, 384)),  # 12 chunks
    ),
    dict(
        name="EcephysTestCase2",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 2), slice(0, 384)),  # 24 chunks
    ),
    dict(
        name="EcephysTestCase3",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 3), slice(0, 384)),  # 36 chunks
    ),
    dict(
        name="EcephysTestCase4",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 4), slice(0, 384)),  # 48 chunks
    ),
    dict(
        name="EcephysTestCase5",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 5), slice(0, 384)),  # 60 chunks
    ),
    dict(
        name="OphysTestCase1",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20), slice(0, 796), slice(0, 512)),  # 1 chunk
    ),
    dict(
        name="OphysTestCase2",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 2), slice(0, 796), slice(0, 512)),  # 2 chunks
    ),
    dict(
        name="OphysTestCase3",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 3), slice(0, 796), slice(0, 512)),  # 3 chunks
    ),
    dict(
        name="OphysTestCase4",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 4), slice(0, 796), slice(0, 512)),  # 4 chunks
    ),
    dict(
        name="OphysTestCase5",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 5), slice(0, 796), slice(0, 512)),  # 5 chunks
    ),
    dict(
        name="IcephysTestCase1",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920),),  # 10 chunks
    ),
    dict(
        name="IcephysTestCase2",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 2),),  # 20 chunks
    ),
    dict(
        name="IcephysTestCase3",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 3),),  # 30 chunks
    ),
    dict(
        name="IcephysTestCase4",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 4),),  # 40 chunks
    ),
    dict(
        name="IcephysTestCase5",
        https_url=get_https_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 5),),  # 50 chunks
    ),
    # dict(
    #     name="EcephysTestCaseIBL",
    #     https_url=get_https_url(
    #         dandiset_id="000717",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    #     ),
    #     object_name="ElectricalSeriesAp",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # ),
)

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_params = (
    dict(
        name="EcephysTestCase1",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144), slice(0, 384)),  # 12 chunks
    ),
    dict(
        name="EcephysTestCase2",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 2), slice(0, 384)),  # 24 chunks
    ),
    dict(
        name="EcephysTestCase3",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 3), slice(0, 384)),  # 36 chunks
    ),
    dict(
        name="EcephysTestCase4",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 4), slice(0, 384)),  # 48 chunks
    ),
    dict(
        name="EcephysTestCase5",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 5), slice(0, 384)),  # 60 chunks
    ),
    dict(
        name="OphysTestCase1",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20), slice(0, 796), slice(0, 512)),  # 1 chunk
    ),
    dict(
        name="OphysTestCase2",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 2), slice(0, 796), slice(0, 512)),  # 2 chunks
    ),
    dict(
        name="OphysTestCase3",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 3), slice(0, 796), slice(0, 512)),  # 3 chunks
    ),
    dict(
        name="OphysTestCase4",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 4), slice(0, 796), slice(0, 512)),  # 4 chunks
    ),
    dict(
        name="OphysTestCase5",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 5), slice(0, 796), slice(0, 512)),  # 5 chunks
    ),
    dict(
        name="IcephysTestCase1",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920),),  # 10 chunks
    ),
    dict(
        name="IcephysTestCase2",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 2),),  # 20 chunks
    ),
    dict(
        name="IcephysTestCase3",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 3),),  # 30 chunks
    ),
    dict(
        name="IcephysTestCase4",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 4),),  # 40 chunks
    ),
    dict(
        name="IcephysTestCase5",
        https_url=get_https_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 5),),  # 50 chunks
    ),
    # dict(
    #     name="EcephysTestCaseIBL",
    #     https_url=get_https_url(
    #         dandiset_id="213889",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.lindi.json",
    #     ),
    #     object_name="ElectricalSeriesAp",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # ),
)

# TODO Test non-consolidated metadata vs consolidated metadata
zarr_params = (
    dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        name="EcephysTestCase1",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144), slice(0, 384)),  # 12 chunks
    ),
    dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        name="EcephysTestCase2",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 2), slice(0, 384)),  # 24 chunks
    ),
    dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        name="EcephysTestCase3",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 3), slice(0, 384)),  # 36 chunks
    ),
    dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        name="EcephysTestCase4",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 4), slice(0, 384)),  # 48 chunks
    ),
    dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        name="EcephysTestCase5",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
        object_name="ElectricalSeries",
        slice_range=(slice(0, 262_144 * 5), slice(0, 384)),  # 60 chunks
    ),
    dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        name="OphysTestCase1",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20), slice(0, 796), slice(0, 512)),  # 1 chunk
    ),
    dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        name="OphysTestCase2",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 2), slice(0, 796), slice(0, 512)),  # 2 chunks
    ),
    dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        name="OphysTestCase3",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 3), slice(0, 796), slice(0, 512)),  # 3 chunks
    ),
    dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        name="OphysTestCase4",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 4), slice(0, 796), slice(0, 512)),  # 4 chunks
    ),
    dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        name="OphysTestCase5",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 20 * 5), slice(0, 796), slice(0, 512)),  # 5 chunks
    ),
    dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        name="IcephysTestCase1",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920),),  # 10 chunks
    ),
    dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        name="IcephysTestCase2",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 2),),  # 20 chunks
    ),
    dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        name="IcephysTestCase3",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 3),),  # 30 chunks
    ),
    dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        name="IcephysTestCase4",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 4),),  # 40 chunks
    ),
    dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        name="IcephysTestCase5",
        https_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 81_920 * 5),),  # 50 chunks
    ),
    # dict(
    #     name="EcephysTestCaseIBL",
    #     https_url=(
    #         "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
    #         "ecephys_625749_2022-08-03_15-15-06_nwb/"
    #         "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
    #     ),
    #     object_name="ElectricalSeriesProbe A-LFP",
    #     slice_range=(slice(0, 30_000), slice(0, 384)),
    # )
)
