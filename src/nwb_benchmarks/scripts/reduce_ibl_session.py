"""Scipt for creating a simpler version of an IBL file."""

import pathlib
import uuid

import lazy_ops  # Not yet a part of core nwb_benchmarks environment; will have to install additionally for now
import neuroconv  # Not yet a part of core nwb_benchmarks environment; will have to install additionally for now
import pynwb

source_folder = pathlib.Path("E:/nwb_benchmark_data")
original_file_path = (
    source_folder / "sub-CSH-ZAD-001_ses-3e7ae7c0-fe8b-487c-9354-036236fa1010-chunking-13653-384_behavior+ecephys.nwb"
)
hdf5_benchmark_dandiset_folder = pathlib.Path("E:/nwb_benchmark_data/000717")  # Path to your local copy of 000717

# Read source NWBFile
io = pynwb.NWBHDF5IO(path=original_file_path, mode="r")
source_nwbfile = io.read()

session_id = source_nwbfile.session_id.split("-")[0]  # Shorten for readability; should still be enough to be unique
source_electrical_series = source_nwbfile.acquisition["ElectricalSeriesAp"]

# rate (Hz) * sec * min * chan * itemsize ; e.g., 30_000 * 60 * 10 * 384 * 2 / 1e9 ~ 14 GB
reduce_frame_bound = 30_000 * 60 * 10
lazy_source_dataset = lazy_ops.DatasetView(dataset=source_electrical_series.data)
reduced_source_dataset = lazy_source_dataset.lazy_slice[(slice(0, reduce_frame_bound), slice(0, 384))]
new_chunks = source_electrical_series.data.chunks  # Just setting equal for now, but in theory could change

# Make output NWBFile - copy over minimal values
simplified_output_nwbfile = pynwb.NWBFile(
    session_description="Minimal copy of a dataset from an IBL electrical series.",
    session_id=session_id,
    session_start_time=source_nwbfile.session_start_time,
    identifier=str(uuid.uuid4()),
    subject=pynwb.file.Subject(
        subject_id=source_nwbfile.subject.subject_id,
        date_of_birth=source_nwbfile.subject.date_of_birth,
        sex=source_nwbfile.subject.sex,
        species=source_nwbfile.subject.species,
    ),
)


simplified_output_time_series = pynwb.ecephys.TimeSeries(
    name="ElectricalSeriesAp",
    description="Reduced copy of the data from the ElectricalSeriesAp object from the IBL session specified in the ID.",
    data=neuroconv.tools.hdmf.SliceableDataChunkIterator(
        data=reduced_source_dataset, buffer_gb=0.5, chunk_shape=new_chunks, display_progress=True
    ),
    rate=source_electrical_series.rate,
    conversion=source_electrical_series.conversion,
    unit=source_electrical_series.unit,
)
simplified_output_nwbfile.add_acquisition(simplified_output_time_series)

# Write the new NWB file
dandi_filename_description = f"_desc-{reduce_frame_bound}-frames-{new_chunks[0]}-by-{new_chunks[1]}-chunking"

simplified_output_subject_folder = hdf5_benchmark_dandiset_folder / "sub-IBL-ecephys"
simplified_output_subject_folder.mkdir(exist_ok=True)
simplified_output_file_path = (
    simplified_output_subject_folder / f"sub-IBL-ecephys_ses-{session_id}{dandi_filename_description}.nwb"
)

with pynwb.NWBHDF5IO(path=simplified_output_file_path, mode="w") as io:
    io.write(simplified_output_nwbfile)
