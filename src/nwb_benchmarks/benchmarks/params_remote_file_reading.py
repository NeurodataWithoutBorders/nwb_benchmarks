from nwb_benchmarks.core import get_s3_url

parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb",
        ),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
    ),
    # IBLTestCase1=dict(
    #     s3_url=get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb"),
    # ),
    # IBLTestCase2 is not the best example for testing a theory about file read; should probably replace with simpler
    # IBLTestCase2=dict(
    #     s3_url=get_s3_url(
    #         dandiset_id="000717",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    #     ),
    # ),
    # ClassicRos3TestCase=dict(s3_url="https://dandiarchive.s3.amazonaws.com/ros3test.nwb"),
)

# Parameters for LINDI when HDF5 files are remote without using an existing LINDI JSON reference file system on
# the remote server (i.e., we create the LINDI JSON file for these in these tests)
lindi_hdf5_parameter_cases = parameter_cases

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file. I.e., here we do not
# to create the JSON but can load it directly from the remote store
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-npI3/sub-npI3_behavior+ecephys.nwb.lindi.json",
        ),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
    ),
    # EcephysTestCase=dict(
    #     s3_url=get_s3_url(
    #         dandiset_id="213889",
    #         dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.lindi.json",
    #     ),
    # ),
)

# TODO Test non-consolidated metadata vs consolidated metadata
# TODO Consider also testing https protocol instead of just s3
zarr_parameter_cases = dict(
    EcephysTestCase=dict(
        # DANDI: 000719 sub-npI3_ses-20190421_behavior+ecephys_rechunk.nwb.zarr
        s3_url="https://dandiarchive.s3.amazonaws.com/zarr/d097af6b-8fd8-4d83-b649-fc6518e95d25/",
    ),
    OphysTestCase=dict(
        # DANDI: 000719 sub-R6_ses-20200206T210000_behavior+ophys_DirectoryStore_rechunked.nwb.zarr
        s3_url="https://dandiarchive.s3.amazonaws.com/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
    ),
    IcephysTestCase=dict(
        # DANDI: 000719 icephys_DS_11_21_24/sub-1214579789_ses-1214621812_icephys_DirectoryStore.nwb.zarr
        s3_url="https://dandiarchive.s3.amazonaws.com/zarr/18e75d22-f527-4051-a4c8-c7e0f1e7dad1/",
    ),
    # AINDTestCase=dict(
    #     s3_url=(
    #         "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
    #         "ecephys_625749_2022-08-03_15-15-06_nwb/"
    #         "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
    #     ),
    # ),
)
