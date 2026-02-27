//! Upsert fast-path, invoked exclusively from within the merge operation.
//! For each conflicting record (matching on join keys), only the source record is kept.
//! All non-conflicting records are appended.

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{Action, EagerSnapshot};
use crate::logstore::LogStoreRef;
use crate::operations::CustomExecuteHandler;
use crate::operations::write::WriterStatsConfig;
use crate::operations::write::execution::write_execution_plan_v2;
use crate::protocol::{DeltaOperation, MergePredicate};
use crate::table::config::TablePropertiesExt;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use arrow_array::Array;
use datafusion::common::JoinType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::DataFrame;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const FILE_PATH_COLUMN: &str = "__delta_rs_path";

#[derive(Default, Debug, Clone, Serialize)]
pub(super) struct UpsertMetrics {
    /// Number of files added to the target table
    pub num_added_files: usize,
    /// Number of files removed from the target table
    pub num_removed_files: usize,
    /// Number of conflicting records detected (rows replaced/updated)
    pub num_conflicting_records: usize,
    /// Time taken to write the output files
    pub write_time_ms: u64,
    /// Time taken to scan the target files
    pub scan_time_ms: u64,
    /// Total execution time for the upsert operation
    pub execution_time_ms: u64,
}

pub(super) struct UpsertBuilder {
    /// The join keys used to identify conflicts between source and target records
    join_keys: Vec<String>,
    /// The source data to upsert into the target table
    source: DataFrame,
    /// The current state of the target table
    snapshot: EagerSnapshot,
    /// Delta log store for handling data files
    log_store: LogStoreRef,
    /// Properties for Parquet writer configuration
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Handler for post-commit hooks
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl UpsertBuilder {
    /// Create a new [`UpsertBuilder`] with required parameters.
    pub(super) fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        join_keys: Vec<String>,
        source: DataFrame,
    ) -> Self {
        Self {
            join_keys,
            source,
            snapshot,
            log_store,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Set the Parquet writer properties for output files.
    pub(super) fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Set additional commit properties for the transaction.
    pub(super) fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set the custom execute handler for post-commit hooks.
    pub(super) fn with_custom_execute_handler(
        mut self,
        handler: Arc<dyn CustomExecuteHandler>,
    ) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Execute the upsert return the updated table together with metrics.
    ///
    /// `state` must already have the log-store registered.
    /// `operation_id` is forwarded to the commit so that the caller can correlate it with
    /// the post-execute hook.
    pub(super) async fn execute(
        self,
        state: Arc<SessionState>,
        operation_id: Uuid,
    ) -> DeltaResult<(DeltaTable, UpsertMetrics)> {
        let exec_start = Instant::now();

        let (actions, mut metrics, partition_filters) =
            self.execute_upsert(state.as_ref(), operation_id).await?;
        let table = self
            .commit_changes(actions, &metrics, &partition_filters, operation_id)
            .await?;

        metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;
        Ok((table, metrics))
    }

    /// Execute the main upsert logic
    async fn execute_upsert(
        &self,
        state: &SessionState,
        operation_id: Uuid,
    ) -> DeltaResult<(
        Vec<Action>,
        UpsertMetrics,
        HashMap<String, HashSet<ScalarValue>>,
    )> {
        let relevant_partition_cols: Vec<String> = self
            .snapshot
            .metadata()
            .partition_columns()
            .iter()
            .filter(|col| self.join_keys.contains(col))
            .cloned()
            .collect();

        let partition_filters: HashMap<String, HashSet<ScalarValue>> = self
            .extract_partition_filters(&relevant_partition_cols)
            .await?;

        // Create target DataFrame with partition filtering
        let target_df = self.create_target_dataframe(state, &partition_filters)?;

        // Check for conflicts between source and target and cache the result for reuse
        let conflicts_df =
            Self::extract_conflicts_dataframe(&target_df, &self.source, &self.join_keys)
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
            let (actions, metrics) = self
                .execute_upsert_with_conflicts(state, &target_df, conflicts_df, operation_id)
                .await?;
            Ok((actions, metrics, partition_filters))
        } else {
            let (actions, metrics) = self.execute_simple_append(state, operation_id).await?;
            Ok((actions, metrics, partition_filters))
        }
    }

    /// For each partition column, extract the unique typed values present in the source DataFrame.
    async fn extract_partition_filters(
        &self,
        columns: &[String],
    ) -> DeltaResult<HashMap<String, HashSet<ScalarValue>>> {
        if columns.is_empty() {
            return Ok(HashMap::new());
        }

        let select_exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
        let batches = self.source.clone().select(select_exprs)?.collect().await?;

        let mut seen: Vec<HashSet<ScalarValue>> =
            (0..columns.len()).map(|_| HashSet::new()).collect();

        for batch in &batches {
            for (col_idx, seen_set) in seen.iter_mut().enumerate() {
                let column = batch.column(col_idx);
                for row_idx in 0..column.len() {
                    if column.is_null(row_idx) {
                        continue;
                    }
                    seen_set.insert(ScalarValue::try_from_array(column.as_ref(), row_idx)?);
                }
            }
        }

        Ok(columns
            .iter()
            .zip(seen)
            .filter(|(_, set)| !set.is_empty())
            .map(|(col_name, set)| (col_name.to_string(), set))
            .collect())
    }

    /// Create a DataFrame for the target table with optional partition filtering.
    fn create_target_dataframe(
        &self,
        state: &SessionState,
        partition_filters: &HashMap<String, HashSet<ScalarValue>>,
    ) -> DeltaResult<DataFrame> {
        let scan_config = crate::delta_datafusion::DeltaScanConfigBuilder::default()
            .with_file_column_name(&FILE_PATH_COLUMN.to_string())
            .with_parquet_pushdown(true)
            .with_schema(self.snapshot.arrow_schema())
            .build(&self.snapshot)?;

        let target_provider = Arc::new(crate::delta_datafusion::DeltaTableProvider::try_new(
            self.snapshot.clone(),
            self.log_store.clone(),
            scan_config,
        )?);

        let mut filters = Vec::new();
        for (column, values) in partition_filters {
            if !values.is_empty() {
                let filter_values: Vec<Expr> = values.iter().map(|sv| lit(sv.clone())).collect();
                filters.push(Expr::InList(InList {
                    expr: Box::new(col(column)),
                    list: filter_values,
                    negated: false,
                }));
            }
        }

        let target_df = DataFrame::new(
            state.clone(),
            datafusion::logical_expr::LogicalPlanBuilder::scan_with_filters(
                datafusion::common::TableReference::bare("target"),
                datafusion::datasource::provider_as_source(target_provider),
                None,
                filters,
            )?
            .build()?,
        );

        Ok(target_df)
    }

    /// Select only the join-key columns from the source for use in the anti-join.
    fn find_conflicts_keys_only(&self) -> DeltaResult<DataFrame> {
        let source_keys: Vec<_> = self.join_keys.iter().map(|k| col(k)).collect();
        self.source
            .clone()
            .select(source_keys)
            .map_err(|e| DeltaTableError::Generic(format!("Error selecting source keys: {e}")))
    }

    /// No conflicts — simply append the source data.
    async fn execute_simple_append(
        &self,
        state: &SessionState,
        operation_id: Uuid,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        let logical_plan = self.source.clone().into_unoptimized_plan();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns().to_vec();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state,
            physical_plan,
            partition_columns,
            self.log_store.object_store(Some(operation_id)),
            Some(self.snapshot.table_properties().target_file_size().get() as usize),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_properties().num_indexed_cols(), None),
            None,
            false,
        )
        .await?;

        let metrics = UpsertMetrics {
            num_added_files: add_actions.len(),
            scan_time_ms: write_metrics.scan_time_ms,
            write_time_ms: write_metrics.write_time_ms,
            ..Default::default()
        };

        Ok((add_actions, metrics))
    }

    /// Conflicts detected — rewrite the affected files and append new rows.
    async fn execute_upsert_with_conflicts(
        &self,
        state: &SessionState,
        target_df: &DataFrame,
        conflicts_df: DataFrame,
        operation_id: Uuid,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        let conflicting_file_names = Self::extract_file_paths_from_conflicts(&conflicts_df).await?;
        let remove_actions = self.files_to_remove(&conflicting_file_names).await?;

        let num_conflicting_records = conflicts_df.count().await?;

        // Narrow the target scan to only the affected files, then drop the path column
        let filtered_target_df =
            Self::filter_conflicting_files(target_df, &conflicting_file_names)?;

        // Anti-join: retain target rows whose join keys don't appear in the source
        let conflicts_keys = self.find_conflicts_keys_only()?;
        let non_conflicting_target =
            self.get_non_conflicting_target_rows(&filtered_target_df, &conflicts_keys)?;
        let result_df = self.union_source_with_target(&non_conflicting_target)?;

        let logical_plan = result_df.into_unoptimized_plan();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns().to_vec();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state,
            physical_plan,
            partition_columns,
            self.log_store.object_store(Some(operation_id)),
            Some(self.snapshot.table_properties().target_file_size().get() as usize),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_properties().num_indexed_cols(), None),
            None,
            false,
        )
        .await?;

        let metrics = UpsertMetrics {
            num_added_files: add_actions.len(),
            num_removed_files: remove_actions.len(),
            num_conflicting_records,
            scan_time_ms: write_metrics.scan_time_ms,
            write_time_ms: write_metrics.write_time_ms,
            ..Default::default()
        };

        let mut all_actions = add_actions;
        all_actions.extend(remove_actions);

        Ok((all_actions, metrics))
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
            ))?
            .drop_columns(&[FILE_PATH_COLUMN])
            .map_err(Into::into)
    }

    async fn files_to_remove(&self, conflicting_file_names: &[String]) -> DeltaResult<Vec<Action>> {
        use futures::stream::StreamExt;

        let mut remove_actions = Vec::new();
        let mut file_stream = self.snapshot.file_views(&self.log_store, None);

        while let Some(file_view) = file_stream.next().await {
            let file_view = file_view?;
            let path = file_view.path().to_string();
            if conflicting_file_names.contains(&path) {
                remove_actions.push(Action::Remove(file_view.remove_action(true)));
            }
        }

        Ok(remove_actions)
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
    fn union_source_with_target(&self, target_no_conflict: &DataFrame) -> DeltaResult<DataFrame> {
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

        let canonical_schema = self.snapshot.arrow_schema();
        let source_aligned = reorder_to_schema(self.source.clone(), canonical_schema.as_ref())?;
        let target_aligned =
            reorder_to_schema(target_no_conflict.clone(), canonical_schema.as_ref())?;

        source_aligned.union(target_aligned).map_err(|e| {
            DeltaTableError::Generic(format!("Union failed after schema alignment: {e}"))
        })
    }

    fn table_from_current_snapshot(&self) -> DeltaTable {
        DeltaTable::new_with_state(
            self.log_store.clone(),
            DeltaTableState::new(self.snapshot.clone()),
        )
    }

    fn build_commit_properties(&self, metrics: &UpsertMetrics) -> CommitProperties {
        let mut app_metadata = self.commit_properties.app_metadata.clone();
        app_metadata.insert("readVersion".to_owned(), self.snapshot.version().into());
        if let Ok(metrics_json) = serde_json::to_value(metrics) {
            app_metadata.insert("operationMetrics".to_owned(), metrics_json);
        }

        let mut commit_properties = self.commit_properties.clone();
        commit_properties.app_metadata = app_metadata;
        commit_properties
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

    fn build_merge_operation(
        &self,
        partition_filters: &HashMap<String, HashSet<ScalarValue>>,
    ) -> DeltaOperation {
        const UPDATE_ACTION_TYPE: &str = "update";
        const INSERT_ACTION_TYPE: &str = "insert";

        DeltaOperation::Merge {
            predicate: Self::build_predicate_expr(&self.join_keys, partition_filters),
            merge_predicate: Self::build_predicate_expr(&self.join_keys, &HashMap::new()),
            matched_predicates: vec![MergePredicate {
                action_type: UPDATE_ACTION_TYPE.to_owned(),
                predicate: None,
            }],
            not_matched_predicates: vec![MergePredicate {
                action_type: INSERT_ACTION_TYPE.to_owned(),
                predicate: None,
            }],
            not_matched_by_source_predicates: vec![],
        }
    }

    /// Commit all changes to the Delta log.
    async fn commit_changes(
        &self,
        actions: Vec<Action>,
        metrics: &UpsertMetrics,
        partition_filters: &HashMap<String, HashSet<ScalarValue>>,
        operation_id: Uuid,
    ) -> DeltaResult<DeltaTable> {
        if actions.is_empty() {
            return Ok(self.table_from_current_snapshot());
        }

        let commit_properties = self.build_commit_properties(metrics);
        let operation = self.build_merge_operation(partition_filters);

        let commit = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(operation_id)
            .with_post_commit_hook_handler(self.custom_execute_handler.clone())
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await?;

        Ok(DeltaTable::new_with_state(
            self.log_store.clone(),
            commit.snapshot(),
        ))
    }
}
