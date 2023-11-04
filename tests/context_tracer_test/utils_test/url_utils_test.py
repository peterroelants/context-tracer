from context_tracer.utils.url_utils import (
    create_query_url,
    parse_url_query_safe,
    urljoin_forward_slash,
)


def test_urljoin_simple() -> None:
    assert (
        urljoin_forward_slash(
            *["https://foo-bar.quux.net", "/foo", "bar", "/bat/", "/quux/"]
        )
        == "https://foo-bar.quux.net/foo/bar/bat/quux/"
    )
    assert (
        urljoin_forward_slash("https://quux.com/", "/path", "to/file///", "//here/")
        == "https://quux.com/path/to/file/here/"
    )
    assert urljoin_forward_slash() == ""
    assert urljoin_forward_slash("//", "beware", "of/this///") == "/beware/of/this///"
    assert (
        urljoin_forward_slash("/leading", "and/", "/trailing/", "slash/")
        == "/leading/and/trailing/slash/"
    )


def test_parse_url_query_safe() -> None:
    dct = parse_url_query_safe("http://www.example.com/?one=1&two=2")
    assert dct["one"] == "1"
    assert dct["two"] == "2"
    assert len(dct) == 2
    dct = parse_url_query_safe(
        "http://www.example.com/?silicon=14&iron=26&inexorable%20progress=vae%20victus"
    )
    assert dct["silicon"] == "14"
    assert dct["iron"] == "26"
    assert dct["inexorable progress"] == "vae victus"
    assert len(dct) == 3
    dct = parse_url_query_safe(
        "http://www.example.com?criterias=member&criterias=issue"
    )
    assert len(dct) == 1
    assert len(dct["criterias"]) == 2
    assert dct["criterias"][0] == "member"
    assert dct["criterias"][1] == "issue"
    dct = parse_url_query_safe(
        "http://www.example.com/?repeated=1&repeated=2&repeated=3&space=jams&space=slams"
    )
    assert len(dct) == 2
    assert len(dct["repeated"]) == 3
    assert dct["repeated"][0] == "1"
    assert dct["repeated"][1] == "2"
    assert dct["repeated"][2] == "3"
    assert len(dct["space"]) == 2
    assert dct["space"][0] == "jams"
    assert dct["space"][1] == "slams"


def test_create_query_url() -> None:
    query_url = create_query_url(
        url="http://www.example.com/",
        params={
            "one": 1,
            "two": 2,
        },
    )
    assert query_url == "http://www.example.com/?one=1&two=2"
    query_url = create_query_url(
        url="http://www.example.com/",
        params={"silicon": 14, "iron": 26, "inexorable progress": "vae victus"},
    )
    assert (
        query_url
        == "http://www.example.com/?silicon=14&iron=26&inexorable%20progress=vae%20victus"
    )
    query_url = create_query_url(
        url="http://www.example.com/",
        params={"silicon": 14, "iron": 26, "inexorable progress": "vae victus"},
    )
    assert (
        query_url
        == "http://www.example.com/?silicon=14&iron=26&inexorable%20progress=vae%20victus"
    )
    query_url = create_query_url(
        url="http://www.example.com",
        params={"criterias": ["member", "issue"]},
    )
    assert query_url == "http://www.example.com?criterias=member&criterias=issue"
    query_url = create_query_url(
        url="http://www.example.com/",
        params={
            "repeated": [1, 2, 3],
            "space": ["jams", "slams"],
        },
    )
    assert (
        query_url
        == "http://www.example.com/?repeated=1&repeated=2&repeated=3&space=jams&space=slams"
    )
