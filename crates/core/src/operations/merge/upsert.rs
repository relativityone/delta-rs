//! Upsert fast-path, invoked exclusively from within the merge operation.
//! For each conflicting record (matching on join keys), only the source record is kept.
//! All non-conflicting records are appended.

use super::{DELETE_COLUMN, OPERATION_COLUMN, SOURCE_COLUMN, TARGET_COLUMN, TARGET_COPY_COLUMN, TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_UPDATE_COLUMN};
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::kernel::EagerSnapshot;
use crate::protocol::{DeltaOperation, MergePredicate};
use crate::{DeltaResult, DeltaTableError};
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::Array;
use datafusion::common::{Column, JoinType};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{col, lit, when, Expr};
use datafusion::prelude::{cast, DataFrame};
use std::collections::{HashMap, HashSet};
use std::iter::once;
use std::ops::Not;
use sqlparser::keywords::Keyword::FILE;

const FILE_PATH_COLUMN: &str = "__delta_rs_path";

pub(super) struct UpsertBuilder {
    /// The join keys used to identify conflicts between source and target records
    join_keys: Vec<String>,
    /// The source data to upsert into the target table
    source: DataFrame,
    /// The current state of the target table
    snapshot: EagerSnapshot,
}

impl UpsertBuilder {
    /// Create a new [`UpsertBuilder`] with required parameters.
    pub(super) fn new(
        snapshot: EagerSnapshot,
        join_keys: Vec<String>,
        source: DataFrame,
    ) -> Self {
        Self {
            join_keys,
            source,
            snapshot,
        }
    }

    pub(super) async fn execute_m_upsert(
        self,
        target: DataFrame,
    ) -> DeltaResult<DataFrame> {
        let conflicts_df =
            Self::extract_conflicts_dataframe(&target, &self.source, &self.join_keys)
                .await?
                .cache()
                .await?;

        let has_conflicts = conflicts_df
            .clone()
            .limit(0, Some(1))?
            .collect()
            .await?
            .is_empty()
            .not();

        if has_conflicts {
            let conflicting_file_names = Self::extract_file_paths_from_conflicts(&conflicts_df).await?;
            // Narrow the target scan to only the affected files, then drop the path column
            let filtered_target_df =
                Self::filter_conflicting_files(&target, &conflicting_file_names)?;

            // Anti-join: retain target rows whose join keys don't appear in the source
            let conflicts_keys = self.find_conflicts_keys_only()?;
            let non_conflicting_target = self.get_non_conflicting_target_rows(&filtered_target_df, &conflicts_keys)?;

            //join source with conflict file paths so we can route to the correct metrics buckets in the presence of conflicts
            // // priny barrier_input for debugging
            // let batches = conflicts_df.clone().collect().await?;
            // println!(
            //     "\nProjected target:\n{}",
            //     arrow_cast::pretty::pretty_format_batches(&batches)?
            // );
            //
            // // priny barrier_input for debugging
            // let batches = self.source.clone().collect().await?;
            // println!(
            //     "\nProjected source:\n{}",
            //     arrow_cast::pretty::pretty_format_batches(&batches)?
            // );

            // let mut sc = self.source.schema().columns();
            // sc.push(Column::from_name(FILE_PATH_COLUMN));
            //
            // let sourcer = &self.source.clone().join(
            //     conflicts_df,
            //     JoinType::Left,
            //     &self.join_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            //     &self.join_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            //     None,
            // )?
            //     .select(
            //         sc
            //     )?;

            let sourcer = self.source.clone().with_column(
                FILE_PATH_COLUMN,
                cast(
                    lit(ScalarValue::Utf8(None)),
                    ArrowDataType::Dictionary(
                        Box::new(ArrowDataType::UInt16),
                        Box::new(ArrowDataType::Utf8),
                    ),
                ),
            )?;

            let source_row_expr = col(SOURCE_COLUMN).is_true();

            self.union_source_with_target(&sourcer, &non_conflicting_target)?.clone()
                .with_column(
                    OPERATION_COLUMN,
                    when(col(FILE_PATH_COLUMN).is_null(), lit(0i32))
                        .when(source_row_expr, lit(1i32))
                        .otherwise(lit(2i32))?,
                )?
                .with_column(DELETE_COLUMN, lit(false))?
                .with_column(
                    TARGET_INSERT_COLUMN,
                    when(
                        col(OPERATION_COLUMN).eq(lit(0i32)),
                        lit(ScalarValue::Boolean(None)),
                    )
                        .otherwise(lit(false))?,
                )?
                .with_column(
                    TARGET_UPDATE_COLUMN,
                    when(
                        col(OPERATION_COLUMN).eq(lit(1i32)),
                        lit(ScalarValue::Boolean(None)),
                    )
                        .otherwise(lit(false))?,
                )?
                .with_column(TARGET_DELETE_COLUMN, lit(false))?
                .with_column(
                    TARGET_COPY_COLUMN,
                    when(
                        col(OPERATION_COLUMN).eq(lit(2i32)),
                        lit(ScalarValue::Boolean(None)),
                    )
                        .otherwise(lit(false))?,
                )
                .map_err(Into::into)
        } else {
            // Pure inserts: no target files to remove.
            self.source
                .clone()
                .with_column(
                    FILE_PATH_COLUMN,
                    cast(
                        lit(ScalarValue::Utf8(None)),
                        ArrowDataType::Dictionary(
                            Box::new(ArrowDataType::UInt16),
                            Box::new(ArrowDataType::Utf8),
                        ),
                    ),
                )?
                .with_column(OPERATION_COLUMN, lit(0i32))?
                .with_column(DELETE_COLUMN, lit(false))?
                .with_column(TARGET_INSERT_COLUMN, lit(ScalarValue::Boolean(None)))?
                .with_column(TARGET_UPDATE_COLUMN, lit(false))?
                .with_column(TARGET_DELETE_COLUMN, lit(false))?
                .with_column(TARGET_COPY_COLUMN, lit(false))
                .map_err(Into::into)
        }
    }

    fn attach_conflict_file_path_to_source(&self, conflicts_df: &DataFrame) -> DeltaResult<DataFrame> {
        // Avoid duplicate unqualified key names after join by aliasing conflict keys.
        let rhs_key_names: Vec<String> = self
            .join_keys
            .iter()
            .map(|k| format!("__delta_rs_conflict_key_{k}"))
            .collect();

        let mut rhs_select_exprs: Vec<Expr> = self
            .join_keys
            .iter()
            .zip(rhs_key_names.iter())
            .map(|(src_key, rhs_key)| col(src_key).alias(rhs_key))
            .collect();
        rhs_select_exprs.push(col(FILE_PATH_COLUMN));

        let conflict_key_to_path = conflicts_df
            .clone()
            .select(rhs_select_exprs)?
            .distinct()?;

        let left_on: Vec<&str> = self.join_keys.iter().map(|s| s.as_str()).collect();
        let right_on: Vec<&str> = rhs_key_names.iter().map(|s| s.as_str()).collect();

        let joined = self
            .source
            .clone()
            .join(
                conflict_key_to_path,
                JoinType::Left,
                &left_on,
                &right_on,
                None,
            )?;

        // Keep source columns + file path and lineage markers used by merge metrics routing.
        let mut projected_cols: Vec<Expr> = self
            .snapshot
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| col(f.name()))
            .collect();
        projected_cols.push(col(FILE_PATH_COLUMN));
        projected_cols.push(col(SOURCE_COLUMN));
        projected_cols.push(col(TARGET_COLUMN));

        joined.select(projected_cols).map_err(Into::into)
    }

    fn union_source_with_target_with_file_path(
        &self,
        source_with_path: &DataFrame,
        target_no_conflict_with_path: &DataFrame,
    ) -> DeltaResult<DataFrame> {
        fn reorder_to_schema(df: DataFrame, columns: &[String]) -> DeltaResult<DataFrame> {
            let exprs: Vec<Expr> = columns.iter().map(col).collect();
            df.select(exprs).map_err(|e| {
                DeltaTableError::Generic(format!(
                    "Failed to reorder DataFrame to reference schema: {e}"
                ))
            })
        }

        let mut canonical_columns: Vec<String> = self
            .snapshot
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        canonical_columns.push(FILE_PATH_COLUMN.to_string());
        canonical_columns.push(SOURCE_COLUMN.to_string());
        canonical_columns.push(TARGET_COLUMN.to_string());

        let source_aligned = reorder_to_schema(source_with_path.clone(), &canonical_columns)?;
        let target_aligned = reorder_to_schema(target_no_conflict_with_path.clone(), &canonical_columns)?;

        source_aligned.union(target_aligned).map_err(|e| {
            DeltaTableError::Generic(format!("Union failed after schema alignment: {e}"))
        })
    }

    /// Select only the join-key columns from the source for use in the anti-join.
    fn find_conflicts_keys_only(&self) -> DeltaResult<DataFrame> {
        let source_keys: Vec<_> = self.join_keys.iter().map(|k| col(k)).collect();
        self.source
            .clone()
            .select(source_keys)
            .map_err(|e| DeltaTableError::Generic(format!("Error selecting source keys: {e}")))
    }

    fn filter_conflicting_files(
        target_df: &DataFrame,
        conflicting_file_names: &[String],
    ) -> DeltaResult<DataFrame> {
        target_df
            .clone()
            .filter(col(FILE_PATH_COLUMN).in_list(
                conflicting_file_names.iter().map(|p| lit(p)).collect(),
                false,
            ))
            //.drop_columns(&[FILE_PATH_COLUMN])
            .map_err(Into::into)
    }

    /// Inner-join target × source on join keys to find conflicting rows.
    ///
    /// Only minimal columns (join keys + file-path) are selected so the result stays small
    /// even for very large tables — only the actual conflict rows are materialised.
    async fn extract_conflicts_dataframe(
        target_df: &DataFrame,
        source: &DataFrame,
        join_keys: &[String],
    ) -> DeltaResult<DataFrame> {
        let mut target_keys: Vec<_> = join_keys.iter().map(|k| col(k)).collect();
        target_keys.push(col(FILE_PATH_COLUMN));
        let target_subset = target_df.clone().select(target_keys)?;

        let source_keys: Vec<_> = join_keys
            .iter()
            .map(|k| col(k).alias(&format!("source_{k}")))
            .collect();
        let source_subset = source.clone().select(source_keys)?;

        let source_key_cols: Vec<_> = join_keys.iter().map(|s| format!("source_{s}")).collect();
        let target_key_cols: Vec<_> = join_keys.iter().map(|s| s.to_string()).collect();

        source_subset
            .join(
                target_subset,
                JoinType::Inner,
                source_key_cols
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
                target_key_cols
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
                None,
            )
            .map_err(Into::into)
    }

    /// Extract the list of unique file paths from the conflicts DataFrame.
    async fn extract_file_paths_from_conflicts(
        conflicts_df: &DataFrame,
    ) -> Result<Vec<String>, DeltaTableError> {
        use std::collections::HashSet;

        let conflicting_paths = conflicts_df
            .clone()
            .select(vec![col(FILE_PATH_COLUMN)])?
            .distinct()?
            .collect()
            .await?;

        let mut conflicting_files = HashSet::new();
        for batch in &conflicting_paths {
            let file_path_col = batch.column(0);
            let as_utf8 =
                arrow::compute::cast(file_path_col.as_ref(), &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| {
                        DeltaTableError::Generic(format!(
                            "Failed to cast file path column to Utf8: {e}"
                        ))
                    })?;
            let str_array = as_utf8
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("cast to Utf8 must yield StringArray");
            for value in str_array.iter().flatten() {
                conflicting_files.insert(value.to_string());
            }
        }

        Ok(conflicting_files.into_iter().collect())
    }

    /// Right-anti-join: return target rows whose join keys do NOT appear in the source.
    fn get_non_conflicting_target_rows(
        &self,
        target_df: &DataFrame,
        conflicts_df: &DataFrame,
    ) -> DeltaResult<DataFrame> {
        let key_strs: Vec<&str> = self.join_keys.iter().map(|s| s.as_str()).collect();
        conflicts_df
            .clone()
            .join(
                target_df.clone(),
                JoinType::RightAnti,
                &key_strs,
                &key_strs,
                None,
            )
            .map_err(Into::into)
    }

    /// Union source data with non-conflicting target rows, aligning column order first.
    fn union_source_with_target(&self, source: &DataFrame, target_no_conflict: &DataFrame) -> DeltaResult<DataFrame> {
        fn reorder_to_schema(
            df: DataFrame,
            reference: &arrow_schema::Schema,
        ) -> DeltaResult<DataFrame> {
            let exprs: Vec<Expr> = reference.fields().iter().map(|f| col(f.name())).collect();
            df.select(exprs).map_err(|e| {
                DeltaTableError::Generic(format!(
                    "Failed to reorder DataFrame to reference schema: {e}"
                ))
            })
        }

        let canonical_schema = target_no_conflict.schema();
        let source_aligned = reorder_to_schema(source.clone(), canonical_schema.as_ref())?;
        let target_aligned =
            reorder_to_schema(target_no_conflict.clone(), canonical_schema.as_ref())?;

        source_aligned.union(target_aligned).map_err(|e| {
            DeltaTableError::Generic(format!("Union failed after schema alignment: {e}"))
        })
    }

    fn build_predicate_expr(
        join_keys: &[String],
        partition_filters: &HashMap<String, HashSet<ScalarValue>>,
    ) -> Option<String> {
        let mut clauses: Vec<Expr> = join_keys
            .iter()
            .map(|k| col(format!("source.{k}")).eq(col(format!("target.{k}"))))
            .collect();

        for (col_name, values) in partition_filters {
            if values.is_empty() {
                continue;
            }
            let list: Vec<Expr> = values.iter().map(|sv| lit(sv.clone())).collect();
            clauses.push(Expr::InList(InList {
                expr: Box::new(col(format!("target.{col_name}"))),
                list,
                negated: false,
            }));
        }

        clauses
            .into_iter()
            .reduce(|acc, clause| acc.and(clause))
            .and_then(|expr| fmt_expr_to_sql(&expr).ok())
    }
}
