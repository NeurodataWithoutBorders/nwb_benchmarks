Running the Benchmarks
======================

Before running the benchmark suite, please ensure you are not running any additional heavy processes in the background to avoid interference or bottlenecks.

To run the full benchmark suite, simply call...

.. code-block::

    nwb_benchmarks run

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

To contribute your results back to the project, all you have to do is `git add` and `commit` the results in the `results` folder.

Then, open a PR to merge the results to the `main` branch.

.. note::

    Each result file should be single to double-digit KB in size; if we ever reach the point where this is prohibitive to store on GitHub itself, then we will investigate other upload strategies and purge the folder from the repository history.
