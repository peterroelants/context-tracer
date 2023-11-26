import uuid

from context_tracer.utils.id_utils import new_uid, uid_to_bytes, uid_to_str


def test_new_id_sequential() -> None:
    id_prev = new_uid()
    for _ in range(100):
        id_next = new_uid()
        assert id_next > id_prev
        id_prev = id_next


def test_span_payload_id_conversion() -> None:
    for uid_bytes in [
        new_uid(),
        uuid.uuid1().bytes,
        uuid.uuid4().bytes,
        new_uid(),
        uuid.uuid4().bytes,
        uuid.uuid4().bytes,
        new_uid(),
    ]:
        assert uid_to_bytes(uid_to_str(uid_bytes)) == uid_bytes
