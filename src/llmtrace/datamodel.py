from typing import Any, Optional

from pydantic import BaseModel, Field

from .utils import get_random_hash, get_utc_timestamp


class TraceNode(BaseModel):
    name: str
    id: str = Field(default_factory=get_random_hash)
    parent: Optional["TraceNode"] = None
    nb_subnodes: int = 0
    timestamp_init_utc: float = Field(default_factory=get_utc_timestamp)
    metadata: Any = None

    def update(
        self, name: str, metadata: Optional[dict] = None
    ) -> tuple["TraceNode", "TraceNode"]:
        parent = TraceNode(
            name=self.name,
            id=self.id,
            parent=self.parent,
            nb_subnodes=self.nb_subnodes + 1,
            timestamp_init_utc=self.timestamp_init_utc,
            metadata=self.metadata,
        )
        child = TraceNode(
            name=name,
            parent=parent,
            metadata=metadata,
        )
        return parent, child

    def get_root(self) -> "TraceNode":
        if self.parent is None:
            return self
        return self.parent.get_root()

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        # id_repr = self.id.hex()[:4]
        id_repr = self.id[:4]
        if self.parent is None:
            return f"(name={self.name} id={id_repr} nb_children={self.nb_subnodes})"
        return f"(name={self.name} id={id_repr} nb_children={self.nb_subnodes} parent={self.parent!r})"

    class Config:
        frozen = True
