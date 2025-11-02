import json
import pathlib
import shutil
import tempfile


def get_benchmarks_home_directory() -> pathlib.Path:
    """Get the home directory for NWB Benchmarks.

    Returns
    -------
    pathlib.Path
        The home directory path.
    """
    home_directory = pathlib.Path.home() / ".nwb_benchmarks"
    home_directory.mkdir(exist_ok=True)
    return home_directory


def get_benchmarks_config_file_path() -> pathlib.Path:
    """Get the configuration file path for NWB Benchmarks.

    Returns
    -------
    pathlib.Path
        The configuration file path.
    """
    config_file_path = get_benchmarks_home_directory() / "config.json"
    return config_file_path


def read_config() -> dict:
    """
    Read the configuration file for NWB Benchmarks.

    Returns
    -------
    dict
        The configuration data.
    """
    config_file_path = get_benchmarks_config_file_path()
    if not config_file_path.exists():
        return {}

    with config_file_path.open(mode="r") as file:
        config = json.load(fp=file)
        return config


def get_cache_directory() -> pathlib.Path:
    """
    Get the cache directory for NWB Benchmarks.

    Returns
    -------
    pathlib.Path or None
        The cache directory path if set in the config file, otherwise it is `~/.cache/nwb_benchmarks`.
    """
    config = read_config()
    cache_directory = config.get("cache_directory", None)
    if cache_directory is not None:
        return pathlib.Path(cache_directory)

    system_cache_directory = pathlib.Path.home() / ".cache"
    system_cache_directory.mkdir(exist_ok=True)
    default_cache_directory = system_cache_directory / "nwb_benchmarks"
    default_cache_directory.mkdir(exist_ok=True)
    return default_cache_directory


def set_cache_directory(cache_directory: pathlib.Path) -> None:
    """
    Set the cache directory for NWB Benchmarks to the default location.

    The default location is `~/.cache/nwb_benchmarks`.
    """
    config = read_config()
    config["cache_directory"] = str(cache_directory)

    config_file_path = get_benchmarks_config_file_path()
    with config_file_path.open(mode="w") as file_stream:
        json.dump(obj=config, fp=file_stream, indent=4)


def get_temporary_directory() -> pathlib.Path:
    """
    Get a temporary directory for NWB Benchmarks.

    Returns
    -------
    pathlib.Path
        The temporary directory path.
    """
    cache_directory = get_cache_directory()
    temporary_directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True, dir=cache_directory)
    return temporary_directory


def get_temporary_file() -> pathlib.Path:
    """
    Get a temporary file for NWB Benchmarks.

    Returns
    -------
    pathlib.Path
        The temporary file path.
    """
    cache_directory = get_cache_directory()
    temporary_file = tempfile.NamedTemporaryFile(delete=False, dir=cache_directory)
    temporary_file.close()
    return pathlib.Path(temporary_file.name)


def clean_cache(ignore_errors: bool = False) -> None:
    """
    Clean the cache directory for NWB Benchmarks.

    Deletes the entire cache directory if it exists.
    """
    cache_directory = get_cache_directory()
    for path in cache_directory.iterdir():
        if path.is_dir():
            shutil.rmtree(path=path, ignore_errors=ignore_errors)
        else:
            path.unlink(missing_ok=True)


def get_persistent_download_directory() -> pathlib.Path:
    """
    Get the persistent download directory for NWB Benchmarks.
    Files downloaded here are not deleted automatically.

    Returns
    -------
    pathlib.Path
        The persistent download directory path.
    """
    benchmarks_home_directory = get_benchmarks_home_directory()
    download_directory = benchmarks_home_directory / "downloads"
    download_directory.mkdir(exist_ok=True)
    return download_directory
