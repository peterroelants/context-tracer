import hashlib
import uuid
from datetime import datetime, timezone


def get_utc_timestamp() -> float:
    return datetime.now(timezone.utc).timestamp()


def get_random_hash() -> str:
    return hashlib.sha256(uuid.uuid1().bytes + uuid.uuid4().bytes).hexdigest()
