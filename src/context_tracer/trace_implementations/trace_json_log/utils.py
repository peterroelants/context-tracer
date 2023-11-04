import hashlib
import uuid


def get_uuid() -> str:
    """
    Return 256-bit UUID.
    """
    return hashlib.sha256(uuid.uuid1().bytes + uuid.uuid4().bytes).hexdigest()
