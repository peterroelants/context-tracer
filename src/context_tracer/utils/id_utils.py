import base64

import uuid6


def new_uid() -> bytes:
    """
    Generate a new id for a span.

    Ids are sequential UUIDs.
    There are two main benefits to using sequential UUIDs:
    - We can now what is the latest span in the trace tree.
    - Better performance when inserting new spans in the SQLite database.

    More info:
    - https://en.wikipedia.org/wiki/Universally_unique_identifier
    - https://docs.python.org/3/library/uuid.html
    - https://github.com/oittaa/uuid6-python/tree/main
    """
    return uuid6.uuid8().bytes


def uid_to_str(uid: bytes) -> str:
    """Convert an id to an urlsafe base64 string without padding."""
    return base64.urlsafe_b64encode(uid).rstrip(b"=").decode("ascii")


def uid_to_bytes(uid: str) -> bytes:
    """Inverse of id_to_str."""
    padding = 4 - (len(uid) % 4)
    return base64.urlsafe_b64decode(uid + ("=" * padding))
