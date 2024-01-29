import abc
import warnings

import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock

warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


class NwbFileReadBase(abc.ABC):
    """
    Base definition of a `read_nwbfile` method using a particular strategy.

    Can be used by child classes for both setup and direct benchmarking.

    The result of calling the `read_nwbfile` method is an instance attribute, "nwbfile", as an open file handle.

    Child classes need to specify:
      - the protocol to use to read the file by overridding the abstract `read_nwbfile`.
    """

    @abc.abstractmethod
    def read_nwbfile(self) -> None:
        raise NotImplementedError


class RemoteNwbFileReadBase(NwbFileReadBase):
    s3_url: str = None  # S3 URL of the NWB asset

    def setup(self):
        assert self.s3_url is not None, "Test must set s3_url class variable"


class FsspecNwbFileRead(RemoteNwbFileReadBase):
    def read_nwbfile(self) -> None:
        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()

        filesystem = fsspec.filesystem("https")

        self.byte_stream = filesystem.open(path=self.s3_url, mode="rb")
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, load_namespaces=True)
        self.nwbfile = self.io.read()


class Ros3NwbFileRead(RemoteNwbFileReadBase):
    def read_nwbfile(self) -> None:
        ros3_form = self.s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
        self.io = pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3")
        self.nwbfile = self.io.read()


class RemfileNWBFileRead(RemoteNwbFileReadBase):
    def read_nwbfile(self) -> None:
        self.byte_stream = remfile.File(url=self.s3_url)
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, load_namespaces=True)
        self.nwbfile = self.io.read()
