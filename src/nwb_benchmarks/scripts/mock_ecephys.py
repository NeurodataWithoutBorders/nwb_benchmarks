"""Scipt for creating a simpler version of an IBL file."""

import pathlib

import pynwb
from pynwb.testing.mock.ecephys import (
    mock_Device,
    mock_ElectricalSeries,
    mock_ElectrodeGroup,
    mock_ElectrodeTable,
)
from pynwb.testing.mock.file import mock_NWBFile

hdf5_benchmark_dandiset_folder = pathlib.Path("E:/nwb_benchmark_data/000717")  # Path to your local copy of 000717


# Create simple mock
session_id = "ecephys1"
nwbfile = mock_NWBFile(
    session_id=session_id,
    subject=pynwb.file.Subject(subject_id="mock", species="Mus musculus", age="P20D", sex="O"),
)

device = mock_Device()
nwbfile.add_device(device)
electrode_group = mock_ElectrodeGroup(device=device)
nwbfile.electrode_groups = {electrode_group.name: electrode_group}
electrodes = mock_ElectrodeTable(group=electrode_group)
nwbfile.electrodes = electrodes


electrical_series = mock_ElectricalSeries(
    electrodes=pynwb.ecephys.DynamicTableRegion(
        name="electrodes", description="", data=list(range(5)), table=electrodes
    )
)
# nwbfile.add_device(electrical_series.electrodes[0]["group"][0].device)
# nwbfile.electrode_groups = {electrical_series.electrodes[0]["group"].name: electrical_series.electrodes[0]["group"]}
# nwbfile.electrodes = electrical_series.electrodes
nwbfile.add_acquisition(electrical_series)

# Write the new NWB file
# Could modify above to produce variations; should document that in dandi description as well as session description
dandi_filename_description = f""

simplified_output_subject_folder = hdf5_benchmark_dandiset_folder / "sub-mock"
simplified_output_subject_folder.mkdir(exist_ok=True)
simplified_output_file_path = (
    simplified_output_subject_folder / f"sub-mock_ses-{session_id}{dandi_filename_description}.nwb"
)

with pynwb.NWBHDF5IO(path=simplified_output_file_path, mode="w") as io:
    io.write(nwbfile)
