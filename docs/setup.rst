Setup
=====

To get started, clone this repo...

.. code-block::

    git clone https://github.com/neurodatawithoutborders/nwb_benchmarks.git
    cd nwb_benchmarks

Setup a completely fresh environment...

.. code-block::

    conda env create --file environments/nwb_benchmarks.yaml --no-default-packages
    conda activate nwb_benchmarks

You will also need to install the custom network tracking software ``tshark`` using `their instructions <https://tshark.dev/setup/install>`_.
