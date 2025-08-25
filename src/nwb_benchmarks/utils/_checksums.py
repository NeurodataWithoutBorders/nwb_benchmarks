import hashlib
import json


def get_dictionary_checksum(dictionary: dict) -> str:
    """Get the deterministic SHA1 hash of the dictionary."""
    sorted_dictionary = dict(sorted(dictionary.items()))
    hasher = hashlib.sha1(string=bytes(json.dumps(obj=sorted_dictionary), encoding="utf-8"))
    checksum = hasher.hexdigest()
    return checksum
