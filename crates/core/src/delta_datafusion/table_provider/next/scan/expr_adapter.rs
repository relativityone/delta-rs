//! Delta-aware physical expression adaptation for parquet scans.

use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{lit, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};

use crate::kernel::schema::cast::cast_array_to_field;

/// A [`PhysicalExprAdapterFactory`] that relaxes nested-field nullability on the
/// logical (table) schema before adapting file columns to it.
///
/// In Delta, `nullable = false` is a write-time invariant: engines such as Spark
/// write parquet files whose nested fields are physically optional even when the
/// table schema declares them non-nullable. DataFusion's default adapter rejects
/// nullable -> non-nullable nested struct field casts at plan time, so we hand it
/// a nullability-relaxed target schema instead. The data is guaranteed non-null
/// by the writer, and the logical output schema is restored above the parquet
/// scan by [`DeltaScanExec`](super::DeltaScanExec)'s transforms.
///
/// Columns whose type still differs after relaxation are cast with delta-rs's own recursive,
/// Map-aware cast instead of DataFusion's, which cannot null-fill a struct nested in a Map
/// value. Upstream DataFusion fix: <https://github.com/apache/datafusion/pull/23914>, tracking
/// <https://github.com/apache/datafusion/issues/20835>. Once this crate builds against a
/// DataFusion release containing that fix, the adapter below can be replaced by
/// `DefaultPhysicalExprAdapterFactory`.
#[derive(Debug)]
pub(crate) struct DeltaPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(DeltaExprAdapter {
            logical_file_schema: Arc::new(relax_schema_nested_nullability(&logical_file_schema)),
            physical_file_schema,
        }))
    }
}

#[derive(Debug)]
struct DeltaExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for DeltaExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(narrowed) = self.try_narrow_struct_cast(&e)? {
                return Ok(Transformed::yes(narrowed));
            }

            let Some(col) = e.downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };

            let Ok(logical_field) = self.logical_file_schema.field_with_name(col.name()) else {
                return Ok(Transformed::no(e));
            };

            let physical_idx = match self.physical_file_schema.index_of(col.name()) {
                Ok(idx) => idx,
                Err(_) if !logical_field.is_nullable() => {
                    return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        col.name()
                    );
                }
                // missing entirely: read it as nulls.
                Err(_) => {
                    let null = ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(lit(null)));
                }
            };

            if logical_field.data_type()
                == self.physical_file_schema.field(physical_idx).data_type()
            {
                if col.index() == physical_idx {
                    return Ok(Transformed::no(e));
                }
                return Ok(Transformed::yes(Arc::new(Column::new(
                    col.name(),
                    physical_idx,
                ))));
            }

            // Types differ. Cast with delta-rs's own recursive, Map-aware cast rather than
            // DataFusion's.
            let resolved = if col.index() == physical_idx {
                col.clone()
            } else {
                Column::new(col.name(), physical_idx)
            };
            Ok(Transformed::yes(Arc::new(DeltaCastColumn {
                expr: Arc::new(resolved),
                target_field: Arc::new(logical_field.clone()),
            })))
        })
        .data()
    }
}

impl DeltaExprAdapter {
    /// Rewrite `get_field(delta_cast(s), 'a', ...)` into `delta_cast(get_field(s, 'a', ...))`,
    /// mirroring `DefaultPhysicalExprAdapter`. Besides avoiding a whole-struct cast, this keeps
    /// the column visible to the parquet scan, which otherwise reports a struct-field predicate
    /// as handled and then silently drops it: <https://github.com/apache/datafusion/issues/24109>.
    /// Map lookups are left alone, since their keys are runtime values.
    fn try_narrow_struct_cast(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(get_field) = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        else {
            return Ok(None);
        };
        let Some((source, key_exprs)) = get_field.args().split_first() else {
            return Ok(None);
        };
        let Some(cast) = source.downcast_ref::<DeltaCastColumn>() else {
            return Ok(None);
        };

        let mut path = Vec::with_capacity(key_exprs.len());
        for key in key_exprs {
            let Some(name) = key
                .downcast_ref::<Literal>()
                .and_then(|l| l.value().try_as_str().flatten())
            else {
                return Ok(None);
            };
            path.push(name);
        }
        let FieldPath::Found(logical_leaf) =
            resolve_field_path(cast.target_field.data_type(), &path)
        else {
            return Ok(None);
        };
        let physical_type = cast.expr.data_type(&self.physical_file_schema)?;
        let physical_leaf = match resolve_field_path(&physical_type, &path) {
            FieldPath::Found(field) => field,
            // The file predates this field, so it reads as null.
            FieldPath::Missing => {
                let null = ScalarValue::Null.cast_to(logical_leaf.data_type())?;
                return Ok(Some(lit(null)));
            }
            FieldPath::Unresolvable => return Ok(None),
        };

        let mut args = Vec::with_capacity(get_field.args().len());
        args.push(Arc::clone(&cast.expr));
        args.extend(key_exprs.iter().cloned());
        let extracted: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(get_field.fun().clone()),
            args,
            &self.physical_file_schema,
            Arc::new(get_field.config_options().clone()),
        )?);

        if physical_leaf.data_type() == logical_leaf.data_type() {
            return Ok(Some(extracted));
        }
        Ok(Some(Arc::new(DeltaCastColumn {
            expr: extracted,
            target_field: Arc::clone(logical_leaf),
        })))
    }
}

enum FieldPath<'a> {
    Found(&'a FieldRef),
    Missing,
    /// Empty path, or a non-struct along the way (e.g. a Map).
    Unresolvable,
}

/// Follows a `get_field` key path through nested structs.
fn resolve_field_path<'a>(dt: &'a DataType, path: &[&str]) -> FieldPath<'a> {
    let (DataType::Struct(fields), Some((first, rest))) = (dt, path.split_first()) else {
        return FieldPath::Unresolvable;
    };
    match fields.iter().find(|f| f.name() == first) {
        None => FieldPath::Missing,
        Some(field) if rest.is_empty() => FieldPath::Found(field),
        Some(field) => resolve_field_path(field.data_type(), rest),
    }
}

/// Evaluates `expr` and casts the result to `target_field`, null-filling fields the file
/// predates.
#[derive(Debug, Clone, Eq)]
struct DeltaCastColumn {
    expr: Arc<dyn PhysicalExpr>,
    target_field: FieldRef,
}

impl PartialEq for DeltaCastColumn {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.target_field == other.target_field
    }
}

impl Hash for DeltaCastColumn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.target_field.hash(state);
    }
}

impl Display for DeltaCastColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "delta_cast({}, {})", self.expr, self.target_field.name())
    }
}

impl PhysicalExpr for DeltaCastColumn {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let cast = cast_array_to_field(&array, &self.target_field, false, true)?;
        Ok(ColumnarValue::Array(cast))
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!("DeltaCastColumn expects exactly one child");
        }
        Ok(Arc::new(Self {
            expr: children.remove(0),
            target_field: Arc::clone(&self.target_field),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// Relax nullability of all *nested* fields. Top-level field nullability is kept
/// as is: the default adapter already tolerates nullable file columns feeding
/// non-nullable top-level table columns, and keeping it strict preserves the
/// "non-nullable column missing from file" error for missing top-level columns.
pub(super) fn relax_schema_nested_nullability(schema: &Schema) -> Schema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| relax_field_type(f.as_ref()))
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Relax nullability within the field's data type, keeping the field's own
/// nullability unchanged.
fn relax_field_type(field: &Field) -> FieldRef {
    Arc::new(
        field
            .clone()
            .with_data_type(relax_nested_nullability(field.data_type())),
    )
}

/// Relax nullability within the field's data type and mark the field itself as
/// nullable.
fn relax_field(field: &FieldRef) -> FieldRef {
    Arc::new(relax_field_type(field).as_ref().clone().with_nullable(true))
}

fn relax_nested_nullability(dt: &DataType) -> DataType {
    match dt {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(relax_field).collect()),
        DataType::List(f) => DataType::List(relax_field(f)),
        DataType::LargeList(f) => DataType::LargeList(relax_field(f)),
        DataType::ListView(f) => DataType::ListView(relax_field(f)),
        DataType::LargeListView(f) => DataType::LargeListView(relax_field(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(relax_field(f), *n),
        DataType::Map(entries, sorted) => {
            // Arrow requires map entries and keys to be non-nullable; only the
            // value field may be relaxed.
            let entries_type = match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => DataType::Struct(Fields::from(vec![
                    relax_field_type(kv[0].as_ref()),
                    relax_field(&kv[1]),
                ])),
                other => relax_nested_nullability(other),
            };
            DataType::Map(
                Arc::new(entries.as_ref().clone().with_data_type(entries_type)),
                *sorted,
            )
        }
        DataType::Dictionary(k, v) => {
            DataType::Dictionary(k.clone(), Box::new(relax_nested_nullability(v)))
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relax_keeps_top_level_nullability_and_relaxes_nested_fields() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "meta",
                DataType::Struct(Fields::from(vec![Field::new(
                    "int_id",
                    DataType::Utf8,
                    false,
                )])),
                true,
            ),
        ]);

        let relaxed = relax_schema_nested_nullability(&schema);

        // Top-level nullability is preserved.
        assert!(!relaxed.field(0).is_nullable());
        assert!(relaxed.field(1).is_nullable());

        // Nested struct field is relaxed to nullable.
        let DataType::Struct(fields) = relaxed.field(1).data_type() else {
            panic!("expected struct");
        };
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn relax_map_keeps_entries_and_key_non_nullable() {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new(
                    "values",
                    DataType::Struct(Fields::from(vec![Field::new("v", DataType::Int32, false)])),
                    false,
                ),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )]);

        let relaxed = relax_schema_nested_nullability(&schema);
        let DataType::Map(entries, _) = relaxed.field(0).data_type() else {
            panic!("expected map");
        };
        assert!(!entries.is_nullable());
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!kv[0].is_nullable(), "map keys must stay non-nullable");
        assert!(kv[1].is_nullable(), "map values should be relaxed");
        let DataType::Struct(value_fields) = kv[1].data_type() else {
            panic!("expected struct values");
        };
        assert!(value_fields[0].is_nullable());
    }

    #[test]
    fn relax_list_variants_relax_element_nullability() {
        let elem = Arc::new(Field::new("item", DataType::Int32, false));
        let schema = Schema::new(vec![
            Field::new("large", DataType::LargeList(elem.clone()), true),
            Field::new("view", DataType::ListView(elem.clone()), true),
            Field::new("large_view", DataType::LargeListView(elem.clone()), true),
            Field::new("fixed", DataType::FixedSizeList(elem.clone(), 2), true),
        ]);

        let relaxed = relax_schema_nested_nullability(&schema);

        for field in relaxed.fields() {
            let inner = match field.data_type() {
                DataType::LargeList(f) | DataType::ListView(f) | DataType::LargeListView(f) => f,
                DataType::FixedSizeList(f, n) => {
                    assert_eq!(*n, 2, "list size must be preserved");
                    f
                }
                other => panic!("unexpected data type: {other:?}"),
            };
            assert!(
                inner.is_nullable(),
                "element of {} should be relaxed",
                field.name()
            );
        }
    }

    #[test]
    fn relax_map_with_non_standard_entries_falls_back_to_generic_relaxation() {
        // Entries struct with three fields does not match the key/value shape,
        // so relaxation falls back to the generic nested handling.
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )]);

        let relaxed = relax_schema_nested_nullability(&schema);
        let DataType::Map(entries, _) = relaxed.field(0).data_type() else {
            panic!("expected map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected struct entries");
        };
        assert!(fields.iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn narrows_cast_under_nested_struct_field_access() {
        use datafusion::config::ConfigOptions;
        use datafusion::functions::core::get_field;

        let inner = |extra: bool| {
            let mut fields = vec![Field::new("name", DataType::Utf8, true)];
            if extra {
                fields.push(Field::new("extra", DataType::Int32, true));
            }
            Field::new("a", DataType::Struct(Fields::from(fields)), true)
        };
        let physical = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![inner(false)])),
            true,
        )]));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                inner(true),
                Field::new("b", DataType::Int32, true),
            ])),
            true,
        )]));

        let access = |keys: &[&str]| -> Arc<dyn PhysicalExpr> {
            let mut args: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("s", 0))];
            args.extend(keys.iter().map(|k| lit(*k)));
            Arc::new(
                ScalarFunctionExpr::try_new(
                    get_field(),
                    args,
                    &logical,
                    Arc::new(ConfigOptions::default()),
                )
                .unwrap(),
            )
        };

        let adapter = DeltaPhysicalExprAdapterFactory
            .create(logical.clone(), physical)
            .unwrap();

        let name = adapter.rewrite(access(&["a", "name"])).unwrap();
        assert!(
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(name.as_ref())
                .is_some_and(|f| f.args()[0].downcast_ref::<Column>().is_some()),
            "expected get_field directly over the column, got {name}"
        );

        let missing = adapter.rewrite(access(&["b"])).unwrap();
        let missing = missing.downcast_ref::<Literal>().expect("null literal");
        assert_eq!(missing.value(), &ScalarValue::Int32(None));
    }

    #[test]
    fn adapts_map_value_struct_missing_a_field() {
        use arrow_array::builder::{
            ArrayBuilder, Int32Builder, MapBuilder, MapFieldNames, StringBuilder, StructBuilder,
        };

        fn map_type(value_fields: Fields) -> DataType {
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Struct(value_fields), true),
                ])),
                false,
            );
            DataType::Map(Arc::new(entries), false)
        }

        let physical_value = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let logical_value = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let physical = Arc::new(Schema::new(vec![Field::new(
            "m",
            map_type(physical_value.clone()),
            true,
        )]));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "m",
            map_type(logical_value),
            true,
        )]));

        let values = StructBuilder::new(
            physical_value,
            vec![Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>],
        );
        let names = MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        };
        let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), values);
        builder.keys().append_value("k");
        builder
            .values()
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        builder.values().append(true);
        builder.append(true).unwrap();
        let batch =
            RecordBatch::try_new(physical.clone(), vec![Arc::new(builder.finish())]).unwrap();

        let adapter = DeltaPhysicalExprAdapterFactory
            .create(logical.clone(), physical)
            .unwrap();
        let rewritten = adapter.rewrite(Arc::new(Column::new("m", 0))).unwrap();
        let out = rewritten
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();

        assert_eq!(out.data_type(), logical.field(0).data_type());
    }
}
