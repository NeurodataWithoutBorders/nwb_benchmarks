"""
Module with helper functions for streaming read access to remote files using various different methods, e.g,
fsspec, remfile, ros3, lindi
"""

import tempfile
import time
import warnings
from typing import Any, Callable, Tuple, Union

import fsspec
import h5py
import hdmf_zarr
import lindi
import pynwb
import remfile
import zarr
from fsspec.asyn import reset_lock
from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.http import HTTPFile
from s3fs.core import S3File

from ..setup import get_temporary_directory

# Useful if running in verbose model
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")

AWS_REGION = "us-east-2"  # DANDI is hosted on us-east-2


def read_hdf5_h5py_fsspec_https_no_cache(
    https_url: str,
) -> Tuple[h5py.File, HTTPFile]:
    """Load the raw HDF5 file using fsspec with an HTTPS filesystem without a cache; does not load into pynwb."""
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")

    byte_stream = filesystem.open(path=https_url, mode="rb")
    file = h5py.File(name=byte_stream, aws_region=bytes(AWS_REGION, "ascii"))
    return (file, byte_stream)


def read_hdf5_h5py_fsspec_https_with_cache(
    https_url: str,
) -> Tuple[h5py.File, HTTPFile, tempfile.TemporaryDirectory]:
    """Load the raw HDF5 file using fsspec with an HTTPS filesystem without a cache; does not load into pynwb."""
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")
    tmpdir = get_temporary_directory()
    filesystem = CachingFileSystem(
        fs=filesystem,
        cache_storage=tmpdir.name,  # Local folder for the cache
    )
    byte_stream = filesystem.open(path=https_url, mode="rb")
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_h5py_fsspec_s3_no_cache(
    https_url: str,
) -> Tuple[h5py.File, S3File]:
    """Load the raw HDF5 file using fsspec with an S3 filesystem without a cache; does not load into pynwb."""
    reset_lock()
    fsspec.get_filesystem_class("s3").clear_instance_cache()
    filesystem = fsspec.filesystem("s3", anon=True)
    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")

    byte_stream = filesystem.open(path=s3_form, mode="rb")
    file = h5py.File(name=byte_stream, aws_region=bytes(AWS_REGION, "ascii"))
    return (file, byte_stream)


def read_hdf5_h5py_fsspec_s3_with_cache(
    https_url: str,
) -> Tuple[h5py.File, S3File, tempfile.TemporaryDirectory]:
    """Load the raw HDF5 file using fsspec with an S3 filesystem without a cache; does not load into pynwb."""
    reset_lock()
    fsspec.get_filesystem_class("s3").clear_instance_cache()
    filesystem = fsspec.filesystem("s3", anon=True)
    tmpdir = get_temporary_directory()
    filesystem = CachingFileSystem(
        fs=filesystem,
        cache_storage=tmpdir.name,  # Local folder for the cache
    )
    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")

    byte_stream = filesystem.open(path=s3_form, mode="rb")
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_pynwb_fsspec_https_no_cache(
    https_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile]:
    """Read an HDF5 NWB file using fsspec with an HTTPS filesystem without a cache."""
    (file, byte_stream) = read_hdf5_h5py_fsspec_https_no_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_hdf5_pynwb_fsspec_https_with_cache(
    https_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile, tempfile.TemporaryDirectory]:
    """Read an HDF5 NWB file using fsspec with an HTTPS filesystem with a cache."""
    (file, byte_stream, tmpdir) = read_hdf5_h5py_fsspec_https_with_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream, tmpdir)


def read_hdf5_pynwb_fsspec_s3_no_cache(
    https_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, S3File]:
    """Read an HDF5 NWB file using fsspec with an S3 filesystem without a cache."""
    (file, byte_stream) = read_hdf5_h5py_fsspec_s3_no_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_hdf5_pynwb_fsspec_s3_with_cache(
    https_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, S3File, tempfile.TemporaryDirectory]:
    """Read an HDF5 NWB file using fsspec with an S3 filesystem with a cache."""
    (file, byte_stream, tmpdir) = read_hdf5_h5py_fsspec_s3_with_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream, tmpdir)


def read_hdf5_h5py_remfile_no_cache(https_url: str) -> Tuple[h5py.File, remfile.File]:
    """Load the raw HDF5 file from an S3 URL using remfile without a cache; does not formally read the NWB file."""
    byte_stream = remfile.File(url=https_url)
    file = h5py.File(name=byte_stream)
    return (file, byte_stream)


def read_hdf5_h5py_remfile_with_cache(https_url: str) -> Tuple[h5py.File, remfile.File, tempfile.TemporaryDirectory]:
    """Load the raw HDF5 file from an S3 URL using remfile with a cache; does not formally read the NWB file."""
    tmpdir = get_temporary_directory()
    disk_cache = remfile.DiskCache(tmpdir.name)
    byte_stream = remfile.File(url=https_url, disk_cache=disk_cache)
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_pynwb_remfile_no_cache(https_url: str) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, remfile.File]:
    """Read an HDF5 NWB file from an S3 URL using remfile without a cache."""
    (file, byte_stream) = read_hdf5_h5py_remfile_no_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_hdf5_pynwb_remfile_with_cache(
    https_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, remfile.File, tempfile.TemporaryDirectory]:
    """Read an HDF5 NWB file from an S3 URL using remfile with a cache."""
    (file, byte_stream, tmpdir) = read_hdf5_h5py_remfile_with_cache(https_url=https_url)
    io = pynwb.NWBHDF5IO(file=file)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream, tmpdir)


def robust_ros3_read(
    command: Callable,
    max_retries: int = 20,
    command_args: Union[list, None] = None,
    command_kwargs: Union[dict, None] = None,
) -> Tuple[Any, int]:
    """
    Attempt the command (usually acting on an S3 IO) up to the number of max_retries using exponential backoff.

    Usually a good idea to use this to wrap any operations performed using the ROS3 driver.

    Returns
    -------
    result : Any
        The object returned by running the comand.
    retries : int
        The number of retries.
    """
    command_args = command_args or []
    command_kwargs = command_kwargs or dict()
    for retries, retry in enumerate(range(max_retries)):
        try:
            result = command(*command_args, **command_kwargs)
            return (result, retries)
        except Exception as exception:
            #  'cannot curl request' can show up in potentially many different error types
            if "curl" not in str(exception):
                raise exception
            time.sleep(0.1 * 2**retry)

    raise TimeoutError(f"Unable to complete the command ({command.__name__}) after {max_retries} attempts!")


def read_hdf5_h5py_ros3(https_url: str, retry: bool = False) -> Tuple[h5py.File, Union[int, None]]:
    """
    Load the raw HDF5 file from an S3 URL using ROS3 driver; does not formally read the NWB file.

    Returns
    -------
    file : h5py.File
        The remote HDF5 file object.
    retries : int
        The number of retries, if `retry` is `True`.
    """
    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    if retry:
        file, retries = robust_ros3_read(
            command=h5py.File,
            command_kwargs=dict(name=s3_form, driver="ros3", aws_region=bytes(AWS_REGION, "ascii")),
        )
    else:
        retries = None
        file = h5py.File(name=s3_form, driver="ros3", aws_region=bytes(AWS_REGION, "ascii"))
    return (file, retries)


def read_hdf5_pynwb_ros3(
    https_url: str, retry: bool = False
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, Union[int, None]]:
    """
    Read an HDF5 NWB file from an S3 URL using the ROS3 driver from h5py.

    Returns
    -------
    NWBFile : pynwb.NWBFile
        The remote NWBFile object.
    NWBHDF5IO : pynwb.NWBHDF5IO
        The open IO object used to open the file. Must be kept to ensure garbage collection does not prematurely close
        the IO.
    retries : int
        The number of retries, if `retry` is `True`.
    """
    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    io = pynwb.NWBHDF5IO(path=s3_form, mode="r", driver="ros3", aws_region=AWS_REGION)

    if retry:
        nwbfile, retries = robust_ros3_read(command=io.read)
    else:
        retries = None
        nwbfile = io.read()
    return (nwbfile, io, retries)


def create_lindi_reference_file_system(https_url: str, outfile_path: str):
    """
    Create a lindi reference file system JSON cache file for a given HDF5 file on S3 (or locally)

    The outfile_path path should end in the '.lindi.json' extension
    """
    # Create the h5py-like client
    max_chunks_to_cache = int(1e9)
    zarr_store_opts = lindi.LindiH5ZarrStoreOpts(num_dataset_chunks_threshold=max_chunks_to_cache)
    client = lindi.LindiH5pyFile.from_hdf5_file(url_or_path=https_url, zarr_store_opts=zarr_store_opts)
    # Generate a reference file system and write it to a file
    client.write_lindi_file(filename=outfile_path)


def read_hdf5_h5py_lindi(rfs: Union[dict, str]) -> lindi.LindiH5pyFile:
    """Open an HDF5 file from an S3 URL using Lindi.

    :param rfs: The LINDI reference file system file. This can be a dictionary or a URL or path to a .lindi.json file.
    """
    # Load the h5py-like client for the reference file system
    client = lindi.LindiH5pyFile.from_lindi_file(url_or_path=rfs)
    return client


def read_hdf5_pynwb_lindi(rfs: Union[dict, str]) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, lindi.LindiH5pyFile]:
    """
    Read an HDF5 NWB file from an S3 URL using LINDI.

    :param rfs: The LINDI reference file system file. This can be a dictionary or a URL or path to a .lindi.json file.
    """
    client = read_hdf5_h5py_lindi(rfs=rfs)
    # Open using pynwb
    io = pynwb.NWBHDF5IO(file=client, mode="r")
    nwbfile = io.read()
    return (nwbfile, io, client)


def read_zarr_zarrpython_https(https_url: str, open_without_consolidated_metadata: bool = False) -> zarr.Group:
    """
    Open a Zarr file from an S3 URL with https protocol using the built-in fsspec support in Zarr.

    Returns
    -------
    file : zarr.Group
       The zarr.Group object representing the opened file
    """
    if open_without_consolidated_metadata:
        zarrfile = zarr.open(store=https_url, mode="r")
    else:
        zarrfile = zarr.open_consolidated(store=https_url, mode="r")
    return zarrfile


def read_zarr_zarrpython_s3(https_url: str, open_without_consolidated_metadata: bool = False) -> zarr.Group:
    """
    Open a Zarr file from an S3 URL with s3 protocol using the built-in fsspec support in Zarr.

    Returns
    -------
    file : zarr.Group
       The zarr.Group object representing the opened file
    """
    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    if open_without_consolidated_metadata:
        zarrfile = zarr.open(store=s3_form, mode="r", storage_options=dict(anon=True))
    else:
        zarrfile = zarr.open_consolidated(store=s3_form, mode="r", storage_options=dict(anon=True))
    return zarrfile


def read_zarr_pynwb_https(https_url: str, mode: str) -> Tuple[pynwb.NWBFile, hdmf_zarr.NWBZarrIO]:
    """
    Read a Zarr NWB file from an S3 URL with https protocol using the built-in fsspec support in Zarr.

    Note: `r-` indicated reading without consolidated metadata, while `r` indicated reading with consolidated.
          `r` should only be used in a benchmark for files that actually have consolidated metadata available,
          for files without consolidated metadata, `hdmf_zarr` automatically reads without consolidated
          metadata if no consolidated metadata is present.

    Returns
    -------
    NWBFile : pynwb.NWBFile
        The remote NWBFile object.
    NWBZarrIO : hdmf_zarr.NWBZarrIO
        The open IO object used to open the file.
    """

    io = hdmf_zarr.NWBZarrIO(https_url, mode=mode)
    nwbfile = io.read()
    return (nwbfile, io)


def read_zarr_pynwb_s3(https_url: str, mode: str) -> Tuple[pynwb.NWBFile, hdmf_zarr.NWBZarrIO]:
    """
    Read a Zarr NWB file from an S3 URL with s3 protocol using the built-in fsspec support in Zarr.

    Note: `r-` indicated reading without consolidated metadata, while `r` indicated reading with consolidated.
          `r` should only be used in a benchmark for files that actually have consolidated metadata available,
          for files without consolidated metadata, `hdmf_zarr` automatically reads without consolidated
          metadata if no consolidated metadata is present.

    Returns
    -------
    NWBFile : pynwb.NWBFile
        The remote NWBFile object.
    NWBZarrIO : hdmf_zarr.NWBZarrIO
        The open IO object used to open the file.
    """

    s3_form = https_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    io = hdmf_zarr.NWBZarrIO(s3_form, mode=mode, storage_options=dict(anon=True))
    nwbfile = io.read()
    return (nwbfile, io)
