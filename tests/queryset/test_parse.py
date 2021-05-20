import pytest

from kirameki.queryset import _parse


@pytest.mark.parametrize(
    "s,expected",
    [
        ("--:def f()", ("f", (), ())),
        ("--   :def    f()   ", ("f", (), ())),
        ("-- :def f(x, y, z)", ("f", ("x", "y", "z"), ())),
        ("-- :def f(x=42)", ("f", ("x",), (42,))),
        ("-- :def f(x, y='foo',)", ("f", ("x", "y"), ("foo",))),
        ("--", None),
        ("-- :def ()", None),
        ("-- :def foo", None),
    ],
)
def test_def_line(s, expected):
    assert _parse.def_line(s) == expected


@pytest.mark.parametrize(
    "s,exc_match",
    [
        ("-- :def 1f()", "'1f' is not a valid identifier"),
        ("-- :def f(1x)", "'1x' is not a valid identifier"),
        ("-- :def f(x=o)", "invalid literal in default"),
        ("-- :def f(x=)", "default must follow"),
        ("-- :def f(x=42, y)", "follows defaults"),
    ],
)
def test_def_line_err(s, exc_match):
    with pytest.raises(ValueError, match=exc_match):
        _parse.def_line(s)


@pytest.mark.parametrize(
    "s,expected",
    [
        ("--:returns :cursor", _parse.ReturnType.CURSOR),
        ("-- :returns  :1  ", _parse.ReturnType.ONE),
    ],
)
def test_returns_line(s, expected):
    assert _parse.returns_line(s) == expected


@pytest.mark.parametrize(
    "s,exc_match",
    [
        ("-- :returns :foobar", "invalid :returns longname"),
        ("-- :returns :$", "invalid :returns shortname"),
    ],
)
def test_returns_line_err(s, exc_match):
    with pytest.raises(ValueError, match=exc_match):
        _parse.returns_line(s)


@pytest.mark.parametrize(
    "s,expected",
    [
        (":x", ("%(x)s", ("x", ))),
        (":x :y", ("%(x)s %(y)s", ("x", "y"))),
        # XXX: consider special-casing ::
        # ("foo::integer", ("foo::integer", ())),
        # ("foo::integer :bar", ("foo::integer %(bar)s", ("bar",))),
        ("'\\:x' :y", ("'\\:x' %(y)s", ("y", ))),
    ],
)
def test_translate_placeholders(s, expected):
    assert _parse.translate_placeholders(s) == expected
