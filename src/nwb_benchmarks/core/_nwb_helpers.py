from typing import Any

import pynwb


def get_object_by_name(nwbfile: pynwb.NWBFile, object_name: str) -> Any:
    """
    Simple helper function to retrieve a neurodata object by its name, if it is unique.

    This method should only be used in the `setup` method of a benchmark class.
    """
    object_names = [neurodata_object.name for neurodata_object in nwbfile.objects.values()]
    assert len(object_names) == len(
        set(object_names)
    ), "Some object names in the NWBFile are not unique! Unable to do a lookup by name."
    assert object_name in object_names, f"The specified object name ({object_name}) is not in the NWBFile."

    return next(
        neurodata_object for neurodata_object in nwbfile.objects.values() if neurodata_object.name == object_name
    )
