# nwb_benchmarks

Benchmark suite for NWB performances using [airspeed velocity](https://asv.readthedocs.io/en/stable/).

To get started,

* Clone this repo...

```
git clone https://github.com/neurodatawithoutborders/nwb_benchmarks.git
cd nwb_benchmarks
```

* Setup the environment...

```
conda env create -f environments/streaming.yaml
conda activate nwb_benchmarks
```

* Configure tracking of our custom machine-dependent parameters by calling...

```
asv machine --yes
python src/nwb_benchmarks/setup/configure_machine.py
```

Please note that we do not currently distinguish any configurations based on your internet; as such there may be difference observed from the same machine in the results database if that machine is a laptop that runs the testing suite on a wide variety of internet qualities.

# TODO: maybe add user-specified section for internet type? Is there no way to grab even the network name?

# TODO: make this machine config a simple entrypoint like the run command

* To run the full benchmark suite, please ensure you are not running any additional heavy processes in the background to avoid interference or bottlenecks, then execute the command...

```
nwb_benchmarks run
```

Many of the current tests can take several minutes to complete; the entire suite can take 10 or more minutes. Grab some coffee, read a book, or better yet (when the suite becomes larger) just leave it to run overnight.

To run only a single benchmark, use the `--bench <benchmark file stem or module+class+test function names>` flag.

# TODO: this might become easier by utilizing the 'pretty names' used to reference tests. Need to explore how child classes trigger from their parents calls.
