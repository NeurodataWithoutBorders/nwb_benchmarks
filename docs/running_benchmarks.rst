Running the Benchmarks
======================

Before running the benchmark suite, please ensure you are not running any additional heavy processes in the background to avoid interference or bottlenecks.

Also, please ensure prior to running the benchmark that all code changes have been committed to your local branch.

For the most stable results, only run the benchmarks on the ``main`` branch.

To run the full benchmark suite, including network tracking tests (which require ``sudo`` on Mac and Linux platforms due to the
use of `psutil net_connections <https://psutil.readthedocs.io/en/latest/#psutil.net_connections>`_), first determine which network
interface you want to monitor (e.g., ``en0``, ``eth0``, etc.). You can typically find this information via your system settings,
or by running commands like ``ifconfig`` or ``ip addr`` in your terminal. On Linux/MacOS, this command will return the default
network interface name for internet connectivity:

.. code-block::

    route get default | awk '/interface:/{print $NF}'

On Windows, in Powershell, you can use:

.. code-block::

    Get-NetAdapter | Where-Object {$_.Status -eq "Up"}

and select the appropriate interface name from the output.

Then, set the environment variable ``NWB_BENCHMARKS_NETWORK_INTERFACE`` to the desired network interface.
For example, in a Unix-like terminal (Linux or macOS), you can do:

.. code-block::

    export NWB_BENCHMARKS_NETWORK_INTERFACE=en0

On Windows, you can use:

.. code-block::

    $env:NWB_BENCHMARKS_NETWORK_INTERFACE="Ethernet"

On Windows, or if ``tshark`` is not installed on the path, you may also need to set the ``TSHARK_PATH`` environment
variable to the absolute path to the ``tshark`` executable (e.g., ``tshark.exe``) on your system.

Then, simply call...

.. code-block::

    sudo -E nwb_benchmarks run

Or drop the ``sudo`` if on Windows.

Many of the current tests can take several minutes to complete; the entire suite will take many times that. Grab some coffee, read a book, or better yet (when the suite becomes larger) just leave it to run overnight.


Additional Flags
----------------

Subset of the Suite
~~~~~~~~~~~~~~~~~~~

To run only a single benchmark suite or a single cast within a benchmark, use the command...

.. code-block::

    nwb_benchmarks run --bench <benchmark file stem or module.class.test function names>

For example,

.. code-block::

    nwb_benchmarks run --bench time_remote_file_reading.HDF5H5pyFileReadBenchmark.time_read_hdf5_h5py_remfile_no_cache

Debug mode
~~~~~~~~~~

If you want to get a full traceback to examine why a new test might be failing, simply add the flag...

.. code-block::

    nwb_benchmarks run --debug

Setting this flag will also override the ``repeat`` parameter of benchmarks and set it to 1, so that you can quickly
iterate on the code and see the results of your changes without having to wait for the full suite to run.

Setting this flag will also not write results to the local cached results directory or upload results to the central
database automatically.

Contributing Results
--------------------

All results should be automatically posted to the central database found on `GitHub <https://github.com/CodyCBakerPhD/nwb-benchmarks-results>`_.

If this fails, you can contribute your results manually by:

.. code-block::

    <Fork https://github.com/CodyCBakerPhD/nwb-benchmarks-results on GitHub>
    git clone https://github.com/<your GitHub username>/nwb-benchmarks-results
    git checkout -b new_results_from_<...>
    <copy results from ~/.cache/nwb_benchmarks/results>
    git commit -m "New results from ...." .
    git push

Then, open a PR to merge the results to the ``main`` branch of the central repo.

.. note::

    When running tests with ``sudo`` the new results may be owned by ``root``. To avoid having to run pre-commit hooks
in sudo you may need to change the owner of the results first, e.g., via ``sudo chown -R <new_owner> results``.

.. note::

    Each result file should be single to double-digit KB in size; if we ever reach the point where this is prohibitive to store on GitHub itself, then we will investigate other upload strategies and purge the folder from the repository history.
