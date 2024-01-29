import warnings

import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock

# Useful if running in verbose mode
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


def read_nwbfile_fsspec(s3_url: str) -> tuple:  # TODO: enhance annotation, add cache option
    reset_lock()
    fsspec.get_filesystem_class("https").clear_instance_cache()
    filesystem = fsspec.filesystem("https")

    byte_stream = filesystem.open(path=s3_url, mode="rb")
    file = h5py.File(name=byte_stream)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)


def read_nwbfile_ros3(s3_url: str) -> tuple:  # TODO: enhance annotation
    ros3_form = s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
    io = pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3")
    nwbfile = io.read()
    return (nwbfile, io)


def read_nwbfile_remfile(s3_url: str) -> tuple:  # TODO: enhance annotation
    byte_stream = remfile.File(url=s3_url)
    file = h5py.File(name=byte_stream)
    io = pynwb.NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return (nwbfile, io, file, byte_stream)
