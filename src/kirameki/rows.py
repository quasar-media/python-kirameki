import sys

from psycopg2 import extensions


class row(tuple):
    def __new__(cls, row_, *, columns, _description=None):
        self = super().__new__(cls, row_)
        self._columns = columns
        self._description = _description
        self._as_dict = {
            sys.intern(name): row_[i] for i, name in enumerate(columns)
        }
        return self

    def as_dict(self):
        return self._as_dict

    def get(self, key, default=None):
        try:
            return self[key]
        except LookupError:
            return default

    def map(self, e=(), **kwargs):
        m = dict(e)
        m.update(kwargs)

        def _mapper(k, v):
            f = m.get(k)
            if f is None:
                return v
            return f(v)

        row_ = tuple(map(_mapper, self._columns, self))
        return type(self)(row_, columns=self._columns)

    def __str__(self):  # pragma: no cover
        return "<{} object at 0x{:x}>".format(type(self).__name__, id(self))

    def __repr__(self):
        return "{}({}, columns={!r})".format(
            type(self).__name__, super().__repr__(), self._columns
        )

    def __getattr__(self, name):
        try:
            # NOTE(auri) could self[name] here but we can skip
            # interning - the name string is almost guaranteed
            # to be interned anyways
            return self._as_dict[name]
        except KeyError:
            raise AttributeError(
                "{!r} object has no attribute {!r}".format(type(self), name)
            ) from None

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._as_dict[sys.intern(key)]
        return super().__getitem__(key)


class RowCursor(extensions.cursor):
    row_class = row

    _columns = None

    def execute(self, query, vars=None):
        self._columns = None
        return super().execute(query, vars)

    def executemany(self, query, vars_list):
        self._columns = None
        return super().executemany(query, vars_list)

    def callproc(self, procname, parameters=None):
        self._columns = None
        return super().callproc(procname, parameters)

    def fetchone(self):
        row = super().fetchone()
        if row is not None:
            self._make_columns()
            return self._to_record(row)
        return None

    def fetchmany(self, size=None):
        rows = super().fetchmany(size)
        self._make_columns()
        return type(rows)(map(self._to_record, rows))

    def fetchall(self):
        rows = super().fetchall()
        self._make_columns()
        return type(rows)(map(self._to_record, rows))

    def _make_columns(self):
        if self._columns is None:
            self._columns = tuple(name for name, *_ in self.description)

    def _to_record(self, row):
        return self.row_class(
            row, columns=self._columns, _description=self.description
        )

    def __iter__(self):
        return self

    def __next__(self):
        self._make_columns()
        return self._to_record(super().__next__())
