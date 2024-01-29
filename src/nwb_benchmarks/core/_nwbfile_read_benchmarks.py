class RemoteNwbFileReadBenchmark:
    """Must be mixed in with a NwbFileReadBase child."""

    def time_remote_nwbfile_read(self):
        self.read_nwbfile()

    def peakmem_remote_nwbfile_read(self):
        self.read_nwbfile()
