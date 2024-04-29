from typing import Any

import pynwb


def get_object_by_name(nwbfile: pynwb.NWBFile, object_name: str) -> Any:
    """
    Simple helper function to retrieve a neurodata object by its name, if it is unique.

    This method should only be used in the `setup` method of a benchmark class.
    """
    # Find all objects that are matching the given name
    matching_objects = [
        (neurodata_object.name, neurodata_object)
        for neurodata_object in nwbfile.objects.values()
        if neurodata_object.name == object_name
    ]
    # Raise an error if the object wasn't found
    if len(matching_objects) == 0:
        raise ValueError(f"The specified object name ({object_name}) is not in the NWBFile.")
    # Make sure that the object we are looking for is unique
    elif len(matching_objects) > 1:
        raise ValueError(f"The specified object name ({object_name}) was found multiple times in the NWBFile.")
    # Return the matching object
    return matching_objects[0][1]
