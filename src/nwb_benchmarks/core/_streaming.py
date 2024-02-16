import time
import warnings
from typing import Callable, Tuple, Union

import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock
from fsspec.implementations.http import HTTPFile

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


def read_hdf5_nwbfile_fsspec_no_cache(
    s3_url: str,
) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO, h5py.File, HTTPFile]:
    """Read an HDF5 NWB file from an S3 URL using fsspec without a cache."""
    (file, byte_stream) = read_hdf5_fsspec_no_cache(s3_url=s3_url)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


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


def robust_ros3_read(
    command: Callable,
    max_retries: int = 10,
    command_args: Union[list, None] = None,
    command_kwargs: Union[dict, None] = None,
):
    """
    Attempt the command (usually acting on an S3 IO) up to the number of max_retries using exponential backoff.

    Usually a good idea to use this to wrap any operations performed using the ROS3 driver.
    """
    command_args = command_args or []
    command_kwargs = command_kwargs or dict()
    for retry in range(max_retries):
        try:
            return command(*command_args, **command_kwargs)
        except Exception as exc:
            if "curl" in str(exc):  # 'cannot curl request' can show up in potentially many different error types
                time.sleep(0.1 * 2**retry)
            else:
                raise exc
    raise TimeoutError(f"Unable to complete the command ({command.__name__}) after {max_retries} attempts!")


def read_hdf5_ros3(s3_url: str) -> h5py.File:
    """Load the raw HDF5 file from an S3 URL using ROS3 driver; does not formally read the NWB file."""
    ros3_form = s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    file = robust_s3_read(command=h5py.File, command_kwargs=dict(name=ros3_form, driver="ros3"))
    return file


def read_hdf5_nwbfile_ros3(s3_url: str) -> Tuple[pynwb.NWBFile, pynwb.NWBHDF5IO]:
    """Read an HDF5 NWB file from an S3 URL using the ROS3 driver from h5py."""
    ros3_form = s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    io = pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3")
    nwbfile = robust_s3_read(command=io.read)
    return (nwbfile, io)
