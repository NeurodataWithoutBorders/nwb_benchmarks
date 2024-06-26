Running the Benchmarks
======================

Before running the benchmark suite, please ensure you are not running any additional heavy processes in the background to avoid interference or bottlenecks.

Also, please ensure prior to running the benchmark that all code changes have been committed to your local branch.

For the most stable results, only run the benchmarks on the ``main`` branch.

To run the full benchmark suite, including network tracking tests (which require ``sudo`` on Mac and AIX platforms due to the
use `psutil net_connections <https://psutil.readthedocs.io/en/latest/#psutil.net_connections>`_), simply call...

.. code-block::

    sudo nwb_benchmarks run

Or drop the ``sudo`` if on Windows.

When running on Windows or if ``tshark`` is not installed on the path, then may also need to set the ``TSHARK_PATH`` environment variable beforehand, which should be the absolute path to the ``tshark`` executable (e.g., ``tshark.exe``) on your system.

Many of the current tests can take several minutes to complete; the entire suite will take many times that. Grab some coffee, read a book, or better yet (when the suite becomes larger) just leave it to run overnight.


Additional Flags
----------------

Subset of the Suite
~~~~~~~~~~~~~~~~~~~

To run only a single benchmark suite (a single file in the ``benchmarks`` directory), use the command...

.. code-block::

    nwb_benchmarks run --bench <benchmark file stem or module+class+test function names>

For example,

.. code-block::

    nwb_benchmarks run --bench time_remote_slicing

Debug mode
~~~~~~~~~~

If you want to get a full traceback to examine why a new test might be failing, simply add the flag...

.. code-block::

    nwb_benchmarks run --debug


Contributing Results
--------------------

To contribute your results back to the project, please use the following workflow...

.. code-block::

    git checkout -b new_results_from_<...>
    git add results/
    git commit -m "New results from ...." .
    git push

Then, open a PR to merge the results to the `main` branch.

.. note::

    When running tests with `sudo` the new results may be owned by `root`. To avoid having to run pre-commit hooks in sudo you may need to change the owner of the results first, e.g., via ``sudo chown -R <new_owner> results

.. note::

    Each result file should be single to double-digit KB in size; if we ever reach the point where this is prohibitive to store on GitHub itself, then we will investigate other upload strategies and purge the folder from the repository history.
