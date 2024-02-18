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
