import attrs
import jsonschema
import numpy as np
import pandas as pd

SPATIAL_POINT_CLASSES = ["SpatialPoint", "BoundSpatialPoint"]

ADD_FUNC_DOCSTSRING = (
    "Add annotation to a local collection. Note that this does not upload annotations."
)


class StagedAnnotations(object):
    def __init__(
        self,
        schema,
        name=None,
        update=False,
        id_field=False,
        table_resolution=None,
        annotation_resolution=None,
        table_name=None,
    ):
        """AnnotationStage object, which helps produce annotations consistent with a CAVE infrastructure
        annotation schema.

        Parameters
        ----------
        schema : dict
            JSONschema object.
        name : _type_, optional
           _description_, by default None
        id_field : bool, optional
            _description_, by default False
        """
        self._schema = schema
        if update:
            id_field = True
        self._id_field = id_field
        self._update = update
        self._table_name = table_name
        self._classes = [x for x in schema["definitions"].keys()]
        self._ref_class = schema.get("$ref").split("/")[-1]
        self._table_resolution = table_resolution
        self._annotation_resolution = annotation_resolution
        self._anno_scaling = None
        if self._table_resolution and self._annotation_resolution:
            self._anno_scaling = [
                y / x
                for x, y in zip(self._table_resolution, self._annotation_resolution)
            ]
        elif self._annotation_resolution:
            raise Warning(
                "No table resolution set. Coordinates cannot be scaled automatically."
            )

        if name is None:
            self.name = self._ref_class
        else:
            self.name = name

        self._required_props = (
            schema["definitions"].get(self._ref_class).get("required")
        )
        self._spatial_pts = {}
        self._convert_pts = {}
        self._props = []
        class_props = schema.get("definitions").get(self._ref_class).get("properties")
        for prop in class_props:
            if "$ref" in class_props.get(prop):
                if (
                    class_props.get(prop)["$ref"].split("/")[-1]
                    in SPATIAL_POINT_CLASSES
                ):
                    self._spatial_pts[prop] = f"{prop}_position"
                    self._convert_pts[f"{prop}_position"] = prop
            self._props.append(prop)
        self._prop_names = self._name_positions()

        self._anno_list = []

        self.add = self._make_anno_func(
            id_field=self._id_field, mixin=(self._build_mixin(),)
        )
        self.add.__doc__ = ADD_FUNC_DOCSTSRING

    def __repr__(self):
        if self._update:
            update = "updated"
        else:
            update = "new"

        if self.table_name:
            table_text = f"table '{self.table_name}'"
        else:
            table_text = f"schema '{self._ref_class}' with no table"
        return f"Staged annotations for {table_text} ({len(self)} {update} annotations)"

    def __len__(self):
        return len(self._anno_list)

    def add_dataframe(self, df):
        """Add multiple annotations via a dataframe. Note that dataframe columns must exactly match fields in the schema (see the "fields" property to check)

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with columns named after schema fields and a row per annotation.
        """
        missing_cols = []
        additional_cols = []
        for col in self.fields_required:
            if col not in df.columns:
                missing_cols.append(col)
        for col in df.columns:
            if col not in self.fields:
                additional_cols.append(col)

        if len(missing_cols) > 0 or len(additional_cols) > 0:
            if len(missing_cols) == 0:
                raise ValueError(
                    f"Dataframe has columns that are not in schema:  {additional_cols}."
                )
            if len(additional_cols) == 0:
                raise ValueError(
                    f"Schema needs columns not in dataframe: {missing_cols}."
                )
            raise ValueError(
                f"Schema needs columns not in dataframe: {missing_cols} and dataframe has columns that do not match fields: {additional_cols}."
            )

        for anno in df.to_dict(orient="records"):
            self.add(**anno)

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, x):
        self._table_name = x

    @property
    def is_update(self):
        return self._update

    @property
    def fields(self):
        if self._id_field:
            return ["id"] + self._prop_names
        else:
            return self._prop_names

    @property
    def fields_required(self):
        if self._id_field:
            return ["id"] + self._name_positions_required()
        else:
            return self._name_positions_required()

    @property
    def annotation_list(self):
        return [self._process_annotation(a, flat=False) for a in self._anno_list]

    @property
    def annotation_dataframe(self):
        return pd.DataFrame.from_records(
            [self._process_annotation(a, flat=True) for a in self._anno_list],
        )

    def clear_annotations(self):
        self._anno_list = []

    def _process_annotation(self, anno, flat=False):
        dflat = attrs.asdict(anno, filter=lambda a, v: v is not None)
        dflat = self._process_spatial(dflat)
        if flat:
            return dflat
        else:
            return self._unflatten_spatial_points(dflat)

    def _build_mixin(self):
        class AddAndValidate(object):
            def __attrs_post_init__(inner_self):
                d = self._process_annotation(inner_self)
                jsonschema.validate(d, self._schema)
                if not isinstance(d.get("id"), int) and self._id_field:
                    raise jsonschema.ValidationError('"id" field must be an integer.')
                self._anno_list.append(inner_self)

        return AddAndValidate

    def _make_anno_func(self, id_field=False, mixin=()):
        cdict = {}

        if id_field:
            cdict["id"] = attrs.field()
        for prop, prop_name in zip(self._props, self._prop_names):
            if prop in self._required_props:
                cdict[prop_name] = attrs.field()
        for prop, prop_name in zip(self._props, self._prop_names):
            if prop not in self._required_props:
                cdict[prop_name] = attrs.field(default=None)

        return attrs.make_class(self.name, cdict, bases=mixin)

    def _name_positions(self):
        return [
            x if x not in self._spatial_pts else f"{x}_position" for x in self._props
        ]

    def _name_positions_required(self):
        return [
            x if x not in self._spatial_pts else f"{x}_position"
            for x in self._required_props
        ]

    def _process_spatial(self, d):
        dout = {}
        for k, v in d.items():
            if isinstance(v, np.ndarray):
                v = list(v)
            if k in self._convert_pts:
                dout[k] = self._process_spatial_point(v)
            else:
                dout[k] = v
        return dout

    def _process_spatial_point(self, v):
        if self._anno_scaling is None:
            return v
        else:
            return [x * y for x, y in zip(v, self._anno_scaling)]

    def _unflatten_spatial_points(self, d):
        dout = {}
        for k, v in d.items():
            if k in self._convert_pts:
                dout[self._convert_pts[k]] = {
                    "position": v,
                }
            else:
                dout[k] = v
        return dout
