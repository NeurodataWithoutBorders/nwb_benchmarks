import time
import warnings
from typing import Any, Callable, Tuple, Union

import fsspec
import h5py
import pynwb
import tempfile
import remfile
from fsspec.asyn import reset_lock
from fsspec.implementations.http import HTTPFile
from fsspec.implementations.cached import CachingFileSystem

# Useful if running in verbose mode
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
) -> Tuple[h5py.File, HTTPFile]:
    """Load the raw HDF5 file from an S3 URL using fsspec without a cache; does not formally read the NWB file."""
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")
	tmpdir= tempfile.TemporaryDirectory()
	fs = CachingFileSystem(
    fs=fs,
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
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile]:
    """Read an HDF5 NWB file from an S3 URL using fsspec without a cache."""
    (file, byte_stream) = read_hdf5_fsspec_no_cache(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    tmpdir= tempfile.TemporaryDirectory()
	fs = CachingFileSystem(
    fs=fs,
    cache_storage=tmpdir.name,  # Local folder for the cache
		)
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

def read_hdf5_remfile_with_cache(s3_url: str) -> Tuple[h5py.File, remfile.File]:
    """Load the raw HDF5 file from an S3 URL using remfile; does not formally read the NWB file."""
    tmpdir= tempfile.TemporaryDirectory()
    disk_cache= remfile.DiskCache(tempdir.name)
    byte_stream = remfile.File(url=s3_url,disk_cache=disk_cache)
    file = h5py.File(name=byte_stream)
    return (file, byte_stream, tmpdir)


def read_hdf5_nwbfile_remfile_with_cache(s3_url: str) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, remfile.File]:
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
