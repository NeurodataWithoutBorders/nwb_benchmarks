"""
Module with helper functions for streaming read access to remote files using various different methods, e.g,
fsspec, remfile, ros3, lindi
"""

import json
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

# Useful if running in verbose model
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


def read_hdf5_fsspec_no_cache(
    s3_url: str,
) -> Tuple[h5py.File, HTTPFile]:
    """Load the raw HDF5 file from an S3 URL using fsspec without a cache; does not formally read the NWB file."""
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")

    byte_stream = filesystem.open(path=s3_url, mode="rb")
    file = h5py.File(name=byte_stream)
    return (file, byte_stream)


def read_hdf5_fsspec_with_cache(
    s3_url: str,
) -> Tuple[h5py.File, HTTPFile, tempfile.TemporaryDirectory]:
    """Load the raw HDF5 file from an S3 URL using fsspec without a cache; does not formally read the NWB file."""
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")
    tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    filesystem = CachingFileSystem(
        fs=filesystem,
        cache_storage=tmpdir.name,  # Local folder for the cache
    )
    byte_stream = filesystem.open(path=s3_url, mode="rb")
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_nwbfile_fsspec_no_cache(
    s3_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile]:
    """Read an HDF5 NWB file from an S3 URL using fsspec without a cache."""
    (file, byte_stream) = read_hdf5_fsspec_no_cache(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_hdf5_nwbfile_fsspec_with_cache(
    s3_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile, tempfile.TemporaryDirectory]:
    """Read an HDF5 NWB file from an S3 URL using fsspec without a cache."""
    (file, byte_stream, tmpdir) = read_hdf5_fsspec_with_cache(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream, tmpdir)


def read_hdf5_remfile(s3_url: str) -> Tuple[h5py.File, remfile.File]:
    """Load the raw HDF5 file from an S3 URL using remfile; does not formally read the NWB file."""
    byte_stream = remfile.File(url=s3_url)
    file = h5py.File(name=byte_stream)
    return (file, byte_stream)


def read_hdf5_nwbfile_remfile(s3_url: str) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, remfile.File]:
    """Read an HDF5 NWB file from an S3 URL using the ROS3 driver from h5py."""
    (file, byte_stream) = read_hdf5_remfile(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_hdf5_remfile_with_cache(s3_url: str) -> Tuple[h5py.File, remfile.File, tempfile.TemporaryDirectory]:
    """Load the raw HDF5 file from an S3 URL using remfile; does not formally read the NWB file."""
    tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    disk_cache = remfile.DiskCache(tmpdir.name)
    byte_stream = remfile.File(url=s3_url, disk_cache=disk_cache)
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_nwbfile_remfile_with_cache(
    s3_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, remfile.File, tempfile.TemporaryDirectory]:
    """Read an HDF5 NWB file from an S3 URL using the ROS3 driver from h5py."""
    (file, byte_stream, tmpdir) = read_hdf5_remfile_with_cache(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
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


def read_hdf5_ros3(s3_url: str, retry: bool = True) -> Tuple[h5py.File, Union[int, None]]:
    """
    Load the raw HDF5 file from an S3 URL using ROS3 driver; does not formally read the NWB file.

    Returns
    -------
    file : h5py.File
        The remote HDF5 file object.
    retries : int
        The number of retries, if `retry` is `True`.
    """
    ros3_form = s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    aws_region = bytes("us-east-2", "ascii")  # TODO: generalize this as an argument if necessary
    if retry:
        file, retries = robust_ros3_read(
            command=h5py.File, command_kwargs=dict(name=ros3_form, driver="ros3", aws_region=aws_region)
        )
    else:
        retries = None
        file = h5py.File(name=ros3_form, driver="ros3", aws_region=aws_region)
    return (file, retries)


def read_hdf5_nwbfile_ros3(s3_url: str, retry: bool = True) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, Union[int, None]]:
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
    ros3_form = s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    io = pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3")

    if retry:
        nwbfile, retries = robust_ros3_read(command=io.read)
    else:
        retries = None
        nwbfile = io.read()
    return (nwbfile, io, retries)


def create_lindi_reference_file_system(s3_url: str, outfile_path: str):
    """
    Create a lindi reference file system JSON cache file for a given HDF5 file on S3 (or locally)

    The output_file path should end in the '.lindi.json' extension
    """
    # Create a read-only Zarr store as a wrapper for the h5 file
    store = lindi.LindiH5ZarrStore.from_file(hdf5_file_name_or_url=s3_url)
    # Generate a reference file system and write it to a file
    store.write_reference_file_system(output_file_name=outfile_path)


def read_hdf5_lindi(rfs: Union[dict, str]) -> lindi.LindiH5pyFile:
    """Open an HDF5 file from an S3 URL using Lindi.

    :param rfs: The LINDI reference file system file. This can be a dictionary or a URL or path to a .lindi.json file.
    """
    # Load the h5py-like client for the reference file system
    client = lindi.LindiH5pyFile.from_reference_file_system(rfs=rfs)
    return client


def read_hdf5_nwbfile_lindi(rfs: Union[dict, str]) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, lindi.LindiH5pyFile]:
    """
    Read an HDF5 NWB file from an S3 URL using LINDI.

    :param rfs: The LINDI reference file system file. This can be a dictionary or a URL or path to a .lindi.json file.
    """
    client = read_hdf5_lindi(rfs=rfs)
    # Open using pynwb
    io = pynwb.NWBHDF5IO(file=client, mode="r")
    nwbfile = io.read()
    return (nwbfile, io, client)


def read_zarr(s3_url: str, open_without_consolidated_metadata: bool = False) -> zarr.Group:
    """
    Open a Zarr file from an S3 URL using the built-in fsspec support in Zarr.

    Returns
    -------
    file : zarr.Group
       The zarr.Group object representing the opened file
    """
    if open_without_consolidated_metadata:
        zarrfile = zarr.open(store=s3_url, mode="r", storage_options=dict(anon=True))
    else:
        zarrfile = zarr.open_consolidated(store=s3_url, mode="r", storage_options=dict(anon=True))
    return zarrfile


def read_zarr_nwbfile(s3_url: str, mode: str) -> Tuple[pynwb.NWBFile, hdmf_zarr.NWBZarrIO]:
    """
    Read a Zarr NWB file from an S3 URL using the built-in fsspec support in Zarr.

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
    io = hdmf_zarr.NWBZarrIO(s3_url, mode=mode, storage_options=dict(anon=True))
    nwbfile = io.read()
    return (nwbfile, io)
