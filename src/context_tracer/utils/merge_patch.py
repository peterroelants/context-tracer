from typing import Any, overload


@overload
def merge_patch(target: Any, patch: dict[str, Any]) -> dict[str, Any]:
    ...


@overload
def merge_patch(target: Any, patch: None) -> None:
    ...


def merge_patch(
    target: dict[str, Any] | Any, patch: dict[str, Any] | None
) -> dict[str, Any] | None:
    """
    JSON Merge Patch (RFC 7396) implementation.

    https://datatracker.ietf.org/doc/html/rfc7396
    """
    if isinstance(patch, dict):
        if not isinstance(target, dict):
            target = {}
        for name, value in patch.items():
            if value is None:
                if name in target:
                    del target[name]
            else:
                target[name] = merge_patch(target.get(name), value)
        return target
    else:
        return patch
