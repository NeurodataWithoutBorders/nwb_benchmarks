
import dataclasses
import pathlib
import packaging
import polars

from ._models import Machine, Environment, Results


def concat_dataclasses_to_parquet(directory: pathlib.Path,
                                  output_directory: pathlib.Path,
                                  dataclass_name: str, 
                                  dataclass: dataclasses.dataclass,
                                  concat_how: str = "diagonal_relaxed",
                                  minimum_version: str = "1.0.0") -> None:
    """Generic function to process any data type (machines, environments, results)
    
    Args:
        directory (pathlib.Path): Path to the root directory containing data subdirectories.
        output_directory (pathlib.Path): Path to the output directory where the parquet file will be saved.
        dataclass_name (str): Name of the data class, used for input and output filenames.
        dataclass: The dataclass type to process (Machine, Environment, Results).
        concat_how (str, optional): How to concatenate dataframes. Defaults to "diagonal_relaxed".
        minimum_version (str, optional): Minimum version of the database to include. Defaults to "1.0.0".
    Returns:

    """
    
    data_frames = []
    data_directory = directory / dataclass_name
    
    for file_path in data_directory.iterdir():
        obj = dataclass.safe_load_from_json(file_path=file_path)
        
        if obj is None:
            continue
        
        data_frame = obj.to_dataframe()

        # filter by minimum version (before concatenation to avoid issues with different results structures)
        # TODO - should environment have a version?
        if "version" in data_frame.columns:
            data_frame = data_frame.filter(
                polars.col("version").map_elements(
                    lambda x: packaging.version.parse(x) >= packaging.version.parse(minimum_version),
                    return_dtype=polars.Boolean
                ))

        data_frames.append(data_frame)
    
    if data_frames:
        database = polars.concat(items=data_frames, how=concat_how)
        output_file_path = output_directory / f"{dataclass_name}.parquet"
        database.write_parquet(file=output_file_path)


def repackage_as_parquet(directory: pathlib.Path, output_directory: pathlib.Path, minimum_version: str = "1.0.0") -> None:
    """Repackage JSON results files as parquet databases for easier querying."""
    
    # Machines
    concat_dataclasses_to_parquet(directory=directory,
                                  output_directory=output_directory,
                                  dataclass_name="machines",
                                  dataclass=Machine,
                                  concat_how="diagonal_relaxed",
                                  minimum_version=minimum_version)

    # Environments
    concat_dataclasses_to_parquet(directory=directory,
                                  output_directory=output_directory,
                                  dataclass_name="environments",
                                  dataclass=Environment,
                                  concat_how="diagonal",
                                  minimum_version=minimum_version)

    # Results
    concat_dataclasses_to_parquet(directory=directory,
                                  output_directory=output_directory,
                                  dataclass_name="results",
                                  dataclass=Results,
                                  concat_how="diagonal_relaxed",
                                  minimum_version=minimum_version)
