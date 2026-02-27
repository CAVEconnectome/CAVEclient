import attrs
import jsonschema
import numpy as np
import pandas as pd

SPATIAL_POINT_CLASSES = ["SpatialPoint", "BoundSpatialPoint"]

ADD_FUNC_DOCSTSRING = (
    "Add annotation to a local collection. Note that this does not upload annotations."
)


class StagedAnnotations(object):
    IS_UPLOADED_FIELD = "_IS_UPLOADED_"
    UPLOADED_ID_FIELD = "_UPLOADED_ID_"
    
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
            Name of the id field, by default False
        update : bool, optional
            Whether these annotations are intended to update existing annotations (True) or create new annotations (False). If True, an "id" field will be added to the annotation class, by default False
        table_resolution : list, optional
            Resolution of the table that these annotations will be uploaded to, in units of nm/px
        annotation_resolution : list, optional
            Resolution of the annotations being added, in units of nm/px. If table_resolution is also provided, annotation coordinates will be automatically scaled to match table resolution. If not provided, coordinates will
            be added as-is, and it is the user's responsibility to ensure they are in the correct units.
        table_name : str, optional
            Name of the table that these annotations will be uploaded to. If not provided, it is the user's responsibility to ensure that the schema provided matches the table they intend to upload to and that it is uploaded to the correct table.
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

    def __repr__(self) -> str:
        if self._update:
            update = "updated"
        else:
            update = "new"

        if self.table_name:
            table_text = f"table '{self.table_name}'"
        else:
            table_text = f"schema '{self._ref_class}' with no table"
        return f"Staged annotations for {table_text} ({len(self)} {update} annotations)"

    def __len__(self) -> int:
        return len(self._anno_list)

    def add_dataframe(self, df) -> None:
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
    def table_name(self) -> str:
        return self._table_name

    @table_name.setter
    def table_name(self, x: str) -> None:
        self._table_name = x

    @property
    def is_update(self) -> bool:
        return self._update

    @property
    def fields(self) -> list:
        if self._id_field:
            return ["id"] + self._prop_names
        else:
            return self._prop_names

    @property
    def fields_required(self) -> list:
        if self._id_field:
            return ["id"] + self._name_positions_required()
        else:
            return self._name_positions_required()

    @property
    def annotation_list(self) -> list:
        return [self._process_annotation(a, flat=False) for a in self._anno_list]

    @property
    def annotation_list_nonuploaded(self) -> list:
        return [
            self._process_annotation(a, flat=False)
            for a in self._anno_list
            if not getattr(a, self.IS_UPLOADED_FIELD, False)
        ]
    
    def _annotation_batches(self, batch_size) -> list:
        """Get a batches of non-uploaded annotations of .
        
        Parameters
        ----------
        batch_size : int
            The number of annotations to include in each batch.
        
        Returns
        -------
        list
            A list of batches, where each batch is a list of annotations that have not been uploaded yet.
        """
        nonuploaded = [a for a in self._anno_list if not getattr(a, self.IS_UPLOADED_FIELD, False)]
        return [nonuploaded[i : i + batch_size] for i in range(0, len(nonuploaded), batch_size)]

    
    @property
    def annotation_dataframe(self) -> pd.DataFrame:
        """Get a dataframe of all annotations, including those that have been uploaded and those that have not."""
        df = pd.DataFrame.from_records([self._process_annotation(a, pop_uploaded_id=False) for a in self._anno_list]) 
        df[self.UPLOADED_ID_FIELD] = df[self.UPLOADED_ID_FIELD].astype('Int64')
        return df
    
    @property
    def annotation_dataframe_nonuploaded(self) -> pd.DataFrame:
        """Get a dataframe of annotations that have not been uploaded yet."""
        df = pd.DataFrame.from_records([self._process_annotation(a, pop_uploaded_id=False) for a in self._anno_list if not getattr(a, self.IS_UPLOADED_FIELD, False)])
        df[self.UPLOADED_ID_FIELD] = df[self.UPLOADED_ID_FIELD].astype('Int64')
        return df

    def clear_annotations(self) -> None:
        """
        Clear all annotations from the internal annotation list. Use with caution, as this cannot be undone.
        """
        self._anno_list = []
    
    def purge_uploaded_annotations(self) -> None:
        """
        Remove annotations that have been uploaded from the internal annotation list.
        """
        self._anno_list = [a for a in self._anno_list if not getattr(a, self.IS_UPLOADED_FIELD, False)]

    def _process_annotation(self, anno, flat=False, pop_is_uploaded=True, pop_uploaded_id=True) -> dict:
        dflat = attrs.asdict(anno, filter=lambda a, v: v is not None)
        if pop_is_uploaded:
            dflat.pop(self.IS_UPLOADED_FIELD, None)
        if pop_uploaded_id:
            dflat.pop(self.UPLOADED_ID_FIELD, None)
        dflat = self._process_spatial(dflat)
        if flat:
            return dflat
        else:
            return self._unflatten_spatial_points(dflat)

    def _build_mixin(self) -> type:
        class AddAndValidate(object):
            def __attrs_post_init__(inner_self):
                d = self._process_annotation(inner_self)
                jsonschema.validate(d, self._schema)
                if not isinstance(d.get("id"), int) and self._id_field:
                    raise jsonschema.ValidationError('"id" field must be an integer.')
                self._anno_list.append(inner_self)

        return AddAndValidate

    def _make_anno_func(self, id_field=False, mixin=()) -> callable:
        cdict = {}
        
        if id_field:
            cdict["id"] = attrs.field()
        for prop, prop_name in zip(self._props, self._prop_names):
            if prop in self._required_props:
                cdict[prop_name] = attrs.field()
        for prop, prop_name in zip(self._props, self._prop_names):
            if prop not in self._required_props:
                cdict[prop_name] = attrs.field(default=None)

        cdict[self.IS_UPLOADED_FIELD] = attrs.field(default=False)
        cdict[self.UPLOADED_ID_FIELD] = attrs.field(type=int, default=None)

        return attrs.make_class(self.name, cdict, bases=mixin)

    def _name_positions(self) -> list:
        return [
            x if x not in self._spatial_pts else f"{x}_position" for x in self._props
        ]

    def _name_positions_required(self) -> list:
        return [
            x if x not in self._spatial_pts else f"{x}_position"
            for x in self._required_props
        ]

    def _process_spatial(self, d) -> dict:
        dout = {}
        for k, v in d.items():
            if isinstance(v, np.ndarray):
                v = list(v)
            if k in self._convert_pts:
                dout[k] = self._process_spatial_point(v)
            else:
                dout[k] = v
        return dout

    def _process_spatial_point(self, v) -> list:
        if self._anno_scaling is None:
            return v
        else:
            return [x * y for x, y in zip(v, self._anno_scaling)]

    def _unflatten_spatial_points(self, d) -> dict:
        dout = {}
        for k, v in d.items():
            if k in self._convert_pts:
                dout[self._convert_pts[k]] = {
                    "position": v,
                }
            else:
                dout[k] = v
        return dout
