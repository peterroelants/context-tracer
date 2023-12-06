from context_tracer.utils.merge_patch import merge_patch


def test_merge_patch():
    assert merge_patch({}, {}) == {}
    assert merge_patch({"a": "b"}, {}) == {"a": "b"}
    assert merge_patch({}, {"a": "b"}) == {"a": "b"}
    assert merge_patch({"a": "b"}, {"a": "c"}) == {"a": "c"}
    assert merge_patch({"a": "b"}, {"b": "c"}) == {"a": "b", "b": "c"}
    object_a = object()
    object_b = object()
    assert merge_patch({"a": object_a}, {"a": object_b}) == {"a": object_b}


def test_merge_patch_example():
    """
    Example from https://datatracker.ietf.org/doc/html/rfc7396#section-3
    """
    a = {
        "title": "Goodbye!",
        "author": {"givenName": "John", "familyName": "Doe"},
        "tags": ["example", "sample"],
        "content": "This will be unchanged",
    }
    b = {
        "title": "Hello!",
        "phoneNumber": "+01-123-456-7890",
        "author": {"familyName": None},
        "tags": ["example"],
    }
    assert merge_patch(a, b) == {
        "title": "Hello!",
        "author": {"givenName": "John"},
        "tags": ["example"],
        "content": "This will be unchanged",
        "phoneNumber": "+01-123-456-7890",
    }


def test_merge_patch_example_test_cases():
    """
    https://datatracker.ietf.org/doc/html/rfc7396#appendix-A
    """
    assert merge_patch({"a": "b"}, {"a": "c"}) == {"a": "c"}
    assert merge_patch({"a": "b"}, {"b": "c"}) == {"a": "b", "b": "c"}
    assert merge_patch({"a": "b"}, {"a": None}) == {}
    assert merge_patch({"a": "b", "b": "c"}, {"a": None}) == {"b": "c"}
    assert merge_patch({"a": ["b"]}, {"a": "c"}) == {"a": "c"}
    assert merge_patch({"a": "c"}, {"a": ["b"]}) == {"a": ["b"]}
    assert merge_patch({"a": {"b": "c"}}, {"a": {"b": "d", "c": None}}) == {
        "a": {"b": "d"}
    }
    assert merge_patch({"a": [{"b": "c"}]}, {"a": [1]}) == {"a": [1]}
    assert merge_patch(["a", "b"], ["c", "d"]) == ["c", "d"]
    assert merge_patch({"a": "b"}, ["c"]) == ["c"]
    assert merge_patch({"a": "foo"}, None) is None
    assert merge_patch({"a": "foo"}, "bar") == "bar"
    assert merge_patch({"e": None}, {"a": 1}) == {"e": None, "a": 1}
    assert merge_patch([1, 2], {"a": "b", "c": None}) == {"a": "b"}
    assert merge_patch({}, {"a": {"bb": {"ccc": None}}}) == {"a": {"bb": {}}}
