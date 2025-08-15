import json
import pathlib


def repackage_as_parquet(results_directory: pathlib.Path, output_directory: pathlib.Path) -> None:
    import polars

    # Machines
    data_frames = []
    for machine_file_path in results_directory.rglob("info_machine*"):
        data_frame = polars.read_json(source=machine_file_path)
        data_frames.append(data_frame)
    machine_database = polars.concat(items=data_frames, how="horizontal")

    machine_database_file_path = output_directory / "machines.parquet"
    machine_database.write_parquet(file=machine_database_file_path)

    # Environments
    data_frame = polars.DataFrame(
        data={
            "blob_index": all_blob_indexes,
            "day": all_days,
            "time": all_times,
            "bytes_sent": all_bytes_sent,
            "indexed_ip": all_indexed_ips,
        }
    )
    data_frame.write_parquet(file=output_file_path)

    # Results
    data_frame = polars.DataFrame(
        data={
            "blob_index": all_blob_indexes,
            "day": all_days,
            "time": all_times,
            "bytes_sent": all_bytes_sent,
            "indexed_ip": all_indexed_ips,
        }
    )
    data_frame.write_parquet(file=output_file_path)
