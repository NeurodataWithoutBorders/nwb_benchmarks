import pathlib

MACHINE_FILE_VERSION = "1.4.1"
DATABASE_VERSION = "2.0.0"

CACHE_DIR = pathlib.Path.home() / ".cache" / "nwb_benchmarks"
CACHE_DIR.mkdir(exist_ok=True)
RESULTS_CACHE_DIR = CACHE_DIR / "results"
RESULTS_CACHE_DIR.mkdir(exist_ok=True)
ENVIRONMENTS_DIR = CACHE_DIR / "environments"
ENVIRONMENTS_DIR.mkdir(exist_ok=True)
MACHINES_DIR = CACHE_DIR / "machines"
MACHINES_DIR.mkdir(exist_ok=True)
LOGS_DIR = CACHE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
