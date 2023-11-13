import hashlib
import uuid


# TODO: Just use default uuid1
def get_uuid() -> bytes:
    """
    Return 256-bit UUID.
    """
    return hashlib.sha256(uuid.uuid1().bytes + uuid.uuid4().bytes).digest()
