import ast
import enum
import re


DEF_LINE = re.compile(
    r"""
    ^ -- \s* :def
         \s+ (?P<name>[^\s]+)
         \s* \( (?P<param_s>.*) \)
         \s* $
    """,
    re.X,
)

RETURNS_LINE = re.compile(
    r"""
    ^ -- \s* :returns
         \s+ : (?:(?P<longname> [A-Za-z]+)
                | (?P<shortname>[\W0-9]))
         \s* $
    """,
    re.X,
)

PLACEHOLDER = re.compile(r"(?<!\\):([^\s:]+)")


class ReturnType(enum.Enum):
    CURSOR = "cursor"
    ONE = "one"
    MANY = "many"
    AFFECTED = "affected"


RETURNS_LONGAMES = {e.value: e for e in ReturnType}

RETURNS_SHORTNAMES = {
    "1": ReturnType.ONE,
    "*": ReturnType.MANY,
    "#": ReturnType.AFFECTED,
}


class Parser:
    def __init__(self):
        self.queries = []
        self._cur_def = None
        self._cur_lines = None

    def parse(self, fp):
        for line in fp.readlines():
            if line.startswith("--"):
                if self._try_def_line(line):
                    continue
                elif self._try_returns_line(line):
                    continue
            if self._cur_def:
                self._cur_lines.append(line)
            elif line.strip():
                raise ValueError("non-meta line before :def")

        self._flush_cur()

        return self.queries

    def _try_def_line(self, s):
        v = def_line(s)
        if not v:
            return False

        self._flush_cur()

        name, params, defaults = v
        self._cur_def = {
            "name": name,
            "params": params,
            "defaults": defaults,
        }
        self._cur_lines = []

        return True

    def _try_returns_line(self, s):
        v = returns_line(s)
        if not v:
            return False

        if not self._cur_def:
            raise ValueError(":returns must follow :def")
        if "returns" in self._cur_def:
            raise ValueError("multiple :returns")

        self._cur_def["returns"] = v

        return True

    def _flush_cur(self):
        if not self._cur_def:
            return

        self._cur_def.setdefault("returns", ReturnType.CURSOR)
        script = "".join(self._cur_lines).rstrip()
        script = translate_placeholders(script)
        self.queries.append((self._cur_def, script))


def def_line(s):
    m = DEF_LINE.match(s)
    if not m:
        return None

    name = m.group("name")
    if not name.isidentifier():
        raise ValueError("{!r} is not a valid identifier".format(name))

    params = []
    defaults = []
    for p in filter(bool, map(str.strip, m.group("param_s").split(","))):
        param, delim, default = p.partition("=")
        param, default = param.strip(), default.strip()

        if not param.isidentifier():
            raise ValueError("{!r} is not a valid identifier".format(param))
        params.append(param)

        if default:
            try:
                default = ast.literal_eval(default)
            except (SyntaxError, ValueError):
                raise ValueError("invalid literal in default: {!r}".format(p))
            defaults.append(default)
        else:
            if delim:
                raise ValueError("default must follow: {!r}".format(p))
            if defaults:
                raise ValueError(
                    "default-less parameter follows defaults: {!r}".format(p)
                )

    return name, tuple(params), tuple(defaults)


def returns_line(s):
    m = RETURNS_LINE.match(s)
    if not m:
        return None

    source = "longname"
    d = RETURNS_LONGAMES
    returns_s = m.group(source)
    if not returns_s:
        source = "shortname"
        d = RETURNS_SHORTNAMES
        returns_s = m.group(source)

    try:
        typ = d[returns_s]
    except KeyError:
        raise ValueError("invalid :returns {}: {!r}".format(source, returns_s))

    return typ


def translate_placeholders(s):
    names = []

    def tr(m):
        (name,) = m.groups()
        if not name.isidentifier():
            raise ValueError("{!r} is not a valid identifier".format(name))
        names.append(name)
        return "%({})s".format(name)

    return PLACEHOLDER.sub(tr, s), tuple(names)


def parse(fp):
    return Parser().parse(fp)
