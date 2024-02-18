Writing Benchmarks
==================

Have an idea for how to speed up read or write from a local or remote NWB file?

This section explains how to write your own benchmark to prove it robustly across platforms, architectures, and environments.


Standard Prefixes
-----------------

Just like how ``pytest`` automatically detects and runs any function or method leading with the keyphrase ``test_``, AirSpeed Velocity runs timing tests for anything prefixed with ``time_``, tracks peak memory via prefix ``peakmem_``, custom values, such as our functions for network traffic, with ``track_`` (this must return the value being tracked), and many others. Check out the full listing in the `primary AirSpeed Velocity documentation <https://asv.readthedocs.io/en/stable/index.html>`_.

A single tracking function should perform only the minimal operations you wish to time. It is also capable of tracking only a single value. The philosophy for this is to avoid interference from cross-measurements; that is, the act of tracking memory of the operation may impact how much overall time it takes that process to complete, so you would not want to simultaneously measure both time and memory.


Class Structure
---------------

A single benchmark suite is a file within the ``benchmarks`` folder. It contains one or more benchmark classes. It is not itself important that the word 'Benchmark' be in the name of the class; only the prefix on the function matters.

The class has several attributes, the most important of which are ``round``, ``repeat``, and ``timeout``. All functions in a class can be repeated in round-robin fashion using ``round > 1``; the philsophy here is to 'average out' variation on the system over time and may not always be relevant to increase. Each function in a suite is repeated ``repeat`` number of times to get an estimate of the standard deviation of the operation. Every function in the suite has at most ``timout`` number of seconds to complete, otherwise it will count as a failure.

Similar to ``unittest.TestCase`` classes, these have a ``setup`` and ``teardown`` method which call before and after execution of every ``round`` and every ``repeat`` for every tracking function (such as timing) in the class. ``setup`` should therefore be as light as possible since it will be repeated so often, though sometimes even a minimal setup can still take time (such as reading a large remote NWB file using a suboptimal method). In some cases, ``setup_cache`` is a method that can be defined, and runs only once per class to precompute some operation, such as the creation of a fake dataset for testing on local disk.

.. note::

    Be careful to assign objects fetched by operations within the tracking functions; otherwise, you may unintentionally track the garbage collection step triggered when the reference count of the return value reaches zero in the namespace. For relatively heavy I/O operations this can be non-negligible.

Finally, you can leverage ``params`` and ``param_names`` to perform a structured iteration over many inputs to the operations. ``param_names`` is a list of length equal to the number of inputs you wish to pass to an operation. ``params`` is a list of lists; the outer list being of equal length to the number of inputs, and each inner list being equal in length to the number of different cases to iterate over.

.. note::

    This structure for ``params`` can be very inconvenient to specify; if you desire a helper function that would instead take a flat list of dictionaries to serve as keyword arguments for all the iteration cases, please request it on our issues board.

For more advanced details, refer to the `primary AirSpeed Velocity documentation <https://asv.readthedocs.io/en/stable/index.html>`_.


Philosophy
----------

In the spirit of PEP8, it was decided from PRs 12, 19, 20, and 21 and ensuing meetings that we should adopt an explicit functionally-based approach to structuring these classes and their methods. This will help make the project  much easier to understand for people outside the team and will even reduce the amount of time it takes our main developers to read benchmarks they have not seen before or have forgotten about over an extended period of time.

This approach means relying as little on inheritance and mixins as possible to reduce the amount of implicit knowledge required to understand a benchmark just by looking at it - instead, all instance methods of the benchmark class should be explicitly defined.

To reduce duplicated code, it is suggested to write standalone helper functions in the ``core`` submodule and then call those functions within the benchmarks. This does mean that some redirection is still required to understand exactly how a given helper function operates, but this was deemed worth it to keep the actual size of benchmarks from inflating.

An example of this philosophy in practice would be as follows. In this example we wish to test how long it takes to both read a small remote NWB file (from the ``s3_url``) using the ``remfile`` method as well as how long it takes to slice ~20 MB of data from the contents of a remote NWB file that has a large amount of series data.

.. code-block:: python

    from nwb_benchmarks.core import read_hdf5_nwbfile_remfile

    class NWBFileReadBenchmark:
        param_names = ["s3_url"]
        params = [
            "https://dandiarchive.s3.amazonaws.com/ros3test.nwb",  # The original small test NWB file
        ]

        def time_read_hdf5_nwbfile_remfile(self, s3_url: str):
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)


.. code-block:: python

    from nwb_benchmarks.core import get_s3_url, read_hdf5_nwbfile_remfile

    class RemfileContinuousSliceBenchmark:
        param_names = ["s3_url", "object_name", "slice_range"]
        params = (
            [
                get_s3_url(  # Yet another helper function for making the NWB file input easier to read
                    dandiset_id="000717",
                    dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
                )
            ],
            ["ElectricalSeriesAp"],
            [(slice(0, 30_000), slice(0, 384))],  # ~23 MB
        )

        def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
            self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
            self.data_to_slice = self.neurodata_object.data

        def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
            """Note: store as self._temp to avoid tracking garbage collection as well."""
            self._temp = self.data_to_slice[slice_range]

Notice how the ``read_hdf5_nwbfile_remfile`` method was used as both the main operating being timed in the first case, then reused in the ``setup`` of the of the second. By following the redirection of the function to its definition, we find it is itself a compound of another helper function for ``remfile`` usage...

.. code-block:: python

    # In nwb_benchmarks/core/_streaming.py

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

and so we managed to save ~5 lines of code for every occurence of this logic in the benchmarks. Good choices of function names are critical to effectively communicating the actions being undertaken. Thorough annotation of signatures is likewise critical to understanding input/output relationships for these functions.
