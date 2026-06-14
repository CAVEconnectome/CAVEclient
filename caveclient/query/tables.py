"""The table/view accessor rebuilt on the query framework.

``TableQuery`` is the per-table object users get from ``client.materialize.tables.<name>``
(or ``.views.<name>``). It exposes columns as handles, accepts keyword filters,
and builds :class:`~caveclient.query.spec.Table` objects that it hands to
``client.materialize.query()`` — so it inherits reference merge/dedup,
frozen-join-via-live, default suffixes, and validation for free.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, replace
from typing import Optional

from .expressions import Column, parse_filter_kwargs
from .kinds import FilterKind
from .serialize import _serialize_value
from .spec import Table


def _did_you_mean(name: str, candidates) -> str:
    close = difflib.get_close_matches(name, list(candidates), n=3)
    return f"; did you mean {' or '.join(repr(c) for c in close)}?" if close else ""


_TYPE_KIND = {
    "integer": FilterKind.NUMERIC,
    "float": FilterKind.NUMERIC,
    "boolean": FilterKind.BOOLEAN,
    "string": FilterKind.STRING,
}


def _scalar_kind(typeval) -> Optional[FilterKind]:
    # schema types can be a bare string or a ["type", "null"] list
    if isinstance(typeval, list):
        typeval = next((t for t in typeval if t != "null"), None)
    return _TYPE_KIND.get(typeval)


def classify_table_schema(schema: dict) -> dict:
    """Column -> FilterKind for an annotation table's JSON schema.

    Bound spatial points expand to ``{pt}_position`` (POSITION), ``{pt}_root_id``
    and ``{pt}_supervoxel_id`` (ID); unbound points to ``{pt}_position``.
    """
    kinds = {"id": FilterKind.NUMERIC}
    name = schema["$ref"].split("/")[-1]
    for fieldname, v in schema["definitions"][name]["properties"].items():
        ref = v.get("$ref", "")
        if ref.endswith("BoundSpatialPoint"):
            kinds[f"{fieldname}_position"] = FilterKind.POSITION
            kinds[f"{fieldname}_supervoxel_id"] = FilterKind.ID
            kinds[f"{fieldname}_root_id"] = FilterKind.ID
        elif ref.endswith("SpatialPoint"):
            kinds[f"{fieldname}_position"] = FilterKind.POSITION
        else:
            kind = _scalar_kind(v.get("format", v.get("type")))
            if kind is not None:
                kinds[fieldname] = kind
    return kinds


def classify_view_schema(schema: dict) -> dict:
    """Column -> FilterKind for a view's flat ``{field: {type}}`` schema.

    Views carry no point structure, so kinds are inferred from name (``*root_id``
    / ``*supervoxel_id`` -> ID, ``*position`` -> POSITION) then type, defaulting
    to UNTYPED (any op, value-shape still checked) when unknown.
    """
    kinds = {}
    for fieldname, v in schema.items():
        typeval = v.get("type") if isinstance(v, dict) else v
        if fieldname.endswith(("root_id", "supervoxel_id")):
            kinds[fieldname] = FilterKind.ID
        elif fieldname.endswith("position"):
            kinds[fieldname] = FilterKind.POSITION
        else:
            kinds[fieldname] = _scalar_kind(typeval) or FilterKind.UNTYPED
    return kinds


@dataclass(frozen=True)
class _Part:
    """One table participating in a (possibly multi-table) query."""

    name: str
    kind: str  # "table" | "view"
    column_kinds: dict
    suffix: Optional[str] = None
    select: Optional[tuple] = None
    merge_reference: bool = True
    join_on: Optional[str] = None


class TableQuery:
    """A filterable handle on a table or view.

    Obtained from ``client.materialize.tables.<name>`` / ``.views.<name>``. Add
    filters by keyword (``tq(size__gt=100)``) or by column-handle expression
    (``tq.size > 100``), optionally ``.join()`` another table, then ``.query()``
    or ``.live_query()``.
    """

    def __init__(self, client, part: _Part, description: Optional[str] = None):
        # one private namespace; everything else is a column handle via __getattr__
        object.__setattr__(self, "_client", client)
        object.__setattr__(self, "_parts", (part,))
        object.__setattr__(self, "_filters", ())
        object.__setattr__(self, "__doc__", description)

    # -- construction helpers ------------------------------------------------

    @classmethod
    def _make(cls, client, parts, filters, description):
        obj = cls.__new__(cls)
        object.__setattr__(obj, "_client", client)
        object.__setattr__(obj, "_parts", tuple(parts))
        object.__setattr__(obj, "_filters", tuple(filters))
        object.__setattr__(obj, "__doc__", description)
        return obj

    def _with(self, filters=None, parts=None):
        return self._make(
            self._client,
            parts if parts is not None else self._parts,
            filters if filters is not None else self._filters,
            self.__doc__,
        )

    @property
    def _primary(self) -> _Part:
        return self._parts[0]

    # -- introspection / column handles --------------------------------------

    @property
    def name(self) -> str:
        return self._primary.name

    @property
    def columns(self) -> dict:
        """Mapping of column name to its filter kind."""
        return dict(self._primary.column_kinds)

    def __getattr__(self, item):
        # column handles; only consulted for names not found normally
        kinds = object.__getattribute__(self, "_parts")[0].column_kinds
        if item in kinds:
            return Column(item, kinds[item], table=self._parts[0].name)
        raise AttributeError(
            f"{self._parts[0].name!r} has no column {item!r}{_did_you_mean(item, kinds)}"
        )

    def __getitem__(self, item) -> Column:
        # column handle by item access (for dynamic / non-identifier names)
        kinds = self._primary.column_kinds
        if item in kinds:
            return Column(item, kinds[item], table=self._primary.name)
        raise KeyError(
            f"{self._primary.name!r} has no column {item!r}{_did_you_mean(item, kinds)}"
        )

    def __dir__(self):
        return list(super().__dir__()) + list(self._primary.column_kinds)

    def __repr__(self):
        joined = "+".join(p.name for p in self._parts)
        return f"<TableQuery {joined} ({len(self._filters)} filters)>"

    # -- filtering -----------------------------------------------------------

    def __call__(self, **kwargs) -> "TableQuery":
        """Add keyword filters (``col=``, ``col__gt=``, ...) to the primary table."""
        new = parse_filter_kwargs(
            self._primary.column_kinds, kwargs, table=self._primary.name
        )
        return self._with(filters=self._filters + new)

    def select(self, *columns) -> "TableQuery":
        """Return only these columns from this table. Each joined table keeps its
        own ``select``, so ``a.select(...).join(b.select(...), ...)`` selects
        per-table."""
        primary = replace(self._primary, select=tuple(columns) or None)
        return self._with(parts=(primary, *self._parts[1:]))

    def join(self, other: "TableQuery", on) -> "TableQuery":
        """Join another table. ``on`` is ``(my_column, other_column)`` or a single
        column name shared by both."""
        if isinstance(on, str):
            my_col, other_col = on, on
        else:
            my_col, other_col = on
        primary = replace(self._primary, join_on=my_col)
        partner = replace(other._primary, join_on=other_col)
        parts = (primary, *self._parts[1:], partner, *other._parts[1:])
        return self._with(parts=parts, filters=self._filters + other._filters)

    # -- execution -----------------------------------------------------------

    def _build_tables(self) -> list:
        by_table = {}
        for f in self._filters:
            tbl = f.column.table or self._primary.name
            field_name = f.op.kwarg_key[: -len("_dict")]  # filter_in_dict -> filter_in
            by_table.setdefault(tbl, {}).setdefault(field_name, {})[f.column.name] = (
                _serialize_value(f)
            )
        tables = []
        for part in self._parts:
            tables.append(
                Table(
                    part.name,
                    join_on=part.join_on,
                    suffix=part.suffix,
                    select=list(part.select) if part.select else None,
                    merge_reference=part.merge_reference,
                    kind=part.kind,
                    **by_table.get(part.name, {}),
                )
            )
        return tables

    def query(self, *exprs, **opts):
        """Run the query. Positional args are column-handle ``Filter`` expressions.

        Keyword options mirror :meth:`MaterializationClient.query`: ``version``,
        ``timestamp``, ``limit``, ``offset``, ``select`` (per primary table),
        ``split_positions``, ``desired_resolution``, ``metadata``, ``get_counts``,
        ``random_sample``, ``allow_missing_lookups``, ``allow_invalid_root_ids``,
        ``allow_version_fallback``, ``datastack_name``.
        """
        select = opts.pop("select", None)
        base = self._with(filters=self._filters + tuple(exprs))
        if select is not None:
            primary = replace(base._primary, select=tuple(select))
            base = base._with(parts=(primary, *base._parts[1:]))
        return base._client.query(base._build_tables(), **opts)

    def live_query(self, timestamp, *exprs, **opts):
        """Query at a timestamp; an alias for ``query(timestamp=...)``."""
        return self.query(*exprs, timestamp=timestamp, **opts)


class _Accessor:
    """Base for the ``tables`` / ``views`` accessors.

    Names are reached by attribute (``mgr.synapses_pni_2``) or item
    (``mgr["synapses_pni_2"]``); both return a fresh :class:`TableQuery`. The
    accessor is built once from prefetched metadata + schemas, so building a
    query object does no extra network.
    """

    _kind = "table"

    def __init__(self, client, descriptions: dict, column_kinds: dict):
        # client is the framework (full) client; queries go via client.materialize
        self._client = client
        self._descriptions = descriptions  # name -> description text
        self._column_kinds = column_kinds  # name -> {column: FilterKind}

    @property
    def names(self) -> list:
        return sorted(self._column_kinds)

    def __len__(self):
        return len(self._column_kinds)

    def __iter__(self):
        return iter(self.names)

    def find(self, substring: str) -> list:
        """Names containing ``substring`` (case-insensitive)."""
        s = substring.lower()
        return [n for n in self.names if s in n.lower()]

    def __contains__(self, name) -> bool:
        return name in self._column_kinds

    def __dir__(self):
        return list(super().__dir__()) + self.names

    def __getitem__(self, name) -> TableQuery:
        if name not in self._column_kinds:
            raise KeyError(
                f"no {self._kind} named {name!r}{_did_you_mean(name, self.names)}"
            )
        part = _Part(
            name=name,
            kind=self._kind,
            column_kinds=self._column_kinds[name],
            merge_reference=(self._kind == "table"),
        )
        return TableQuery(
            self._client.materialize, part, description=self._descriptions.get(name)
        )

    def __getattr__(self, name) -> TableQuery:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"no {self._kind} named {name!r}"
                f"{_did_you_mean(name, self.names)} (see .names or .find(...))"
            )

    def _repr_html_(self):
        rows = "".join(
            f"<li><b>{n}</b>: {(self._descriptions.get(n) or '')[:120]}</li>"
            for n in self.names
        )
        return f"<details><summary>{len(self.names)} {self._kind}s</summary><ul>{rows}</ul></details>"


class TableManager(_Accessor):
    """``client.materialize.tables`` — query annotation tables by name."""

    _kind = "table"

    @classmethod
    def build(cls, client, tables_metadata, schemas: dict) -> "TableManager":
        # get_tables_metadata returns a list of dicts; accept a {name: meta} dict too
        items = (
            tables_metadata.values()
            if isinstance(tables_metadata, dict)
            else tables_metadata
        )
        descriptions, column_kinds = {}, {}
        for meta in items:
            name = meta.get("table_name") or meta.get("table")
            if not name:
                continue
            descriptions[name] = meta.get("description")
            schema = schemas.get(meta.get("schema_type") or meta.get("schema"))
            if schema:
                try:
                    column_kinds[name] = classify_table_schema(schema)
                except (KeyError, TypeError):
                    continue
        return cls(client, descriptions, column_kinds)


class ViewManager(_Accessor):
    """``client.materialize.views`` — query views by name."""

    _kind = "view"

    @classmethod
    def build(cls, client, view_metadata: dict, view_schemas: dict) -> "ViewManager":
        descriptions, column_kinds = {}, {}
        for name, meta in view_metadata.items():
            descriptions[name] = (
                meta.get("description") if isinstance(meta, dict) else None
            )
            schema = view_schemas.get(name)
            if schema:
                column_kinds[name] = classify_view_schema(schema)
        return cls(client, descriptions, column_kinds)
