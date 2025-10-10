//! Upsert data from a source DataFrame into a target Delta Table.
//! For each conflicting record (e.g., matching on primary key), only the source record is kept.
//! All non-conflicting records are appended.

use arrow_array::Array;
use datafusion::execution::SessionState;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common::JoinType;
use datafusion_expr::expr::InList;
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::delta_datafusion::DeltaSessionConfig;
use crate::delta_datafusion::{register_store, DataFusionMixins};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, TableReference, PROTOCOL};
use crate::kernel::{Action, Remove};
use crate::logstore::LogStoreRef;
use crate::operations::write::execution::write_execution_plan_v2;
use crate::operations::write::WriterStatsConfig;
use crate::protocol::SaveMode;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use datafusion::logical_expr::{col, lit, Expr};

#[derive(Default, Debug, Clone, Serialize)]
/// Metrics collected during the Upsert operation
pub struct UpsertMetrics {
    /// Number of files added to the target table
    pub num_added_files: usize,
    /// Number of files removed from the target table
    pub num_removed_files: usize,
    /// Time taken to execute the entire operation
    pub write_time_ms: u64,
    /// Time taken to scan the target files
    pub scan_time_ms: u64,
    /// Total execution time for the upsert operation
    pub execution_time_ms: u64,
}

/// Builder for configuring and executing an upsert operation
pub struct UpsertBuilder {
    /// The join keys used to identify conflicts between source and target records
    join_keys: Vec<String>,
    /// The source data to upsert into the target table
    source: DataFrame,
    /// The current state of the target table
    snapshot: DeltaTableState,
    /// Delta log store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state for executing the plans
    state: Option<Arc<SessionState>>,
    /// Properties for Parquet writer configuration
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl UpsertBuilder {
    /// Create a new UpsertBuilder with required parameters
    pub fn new(
        log_store: LogStoreRef,
        snapshot: DeltaTableState,
        join_keys: Vec<String>,
        source: DataFrame,
    ) -> Self {
        Self {
            join_keys,
            source,
            snapshot,
            log_store,
            state: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Set the Datafusion session state to use for plan execution
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(Arc::from(state));
        self
    }

    /// Set the Parquet writer properties for output files
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Set additional commit properties for the transaction
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl super::Operation<()> for UpsertBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    fn get_custom_execute_handler(&self) -> Option<Arc<dyn super::CustomExecuteHandler>> {
        None
    }
}

impl std::future::IntoFuture for UpsertBuilder {
    type Output = DeltaResult<(DeltaTable, UpsertMetrics)>;
    type IntoFuture = futures::future::BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let exec_start = Instant::now();

            // Validate table state and protocol
            Self::validate_table_state(&self.snapshot)?;

            // Get or create session state
            let state = self.get_or_create_session_state();

            // Execute the upsert operation
            let (actions, mut metrics) = self.execute_upsert(state).await?;

            // Commit the changes
            let table = self.commit_changes(actions, &metrics).await?;

            metrics.execution_time_ms =
                Instant::now().duration_since(exec_start).as_millis() as u64;
            Ok((table, metrics))
        })
    }
}

const FILE_PATH_COLUMN: &'static str = "__delta_rs_path";

impl UpsertBuilder {
    /// Validate that the table is in a valid state for upsert operations
    fn validate_table_state(snapshot: &DeltaTableState) -> DeltaResult<()> {
        PROTOCOL.can_write_to(&snapshot.snapshot)?;

        if !snapshot.load_config().require_files {
            return Err(DeltaTableError::NotInitializedWithFiles("UPSERT".into()));
        }

        Ok(())
    }

    /// Get the existing session state or create a new one
    fn get_or_create_session_state(&self) -> Arc<SessionState> {
        match &self.state {
            Some(state) => Arc::clone(state),
            None => {
                let config: datafusion::execution::context::SessionConfig =
                    DeltaSessionConfig::default().into();
                let session = SessionContext::new_with_config(config);
                register_store(self.log_store.clone(), session.runtime_env());
                Arc::new(session.state())
            }
        }
    }

    /// Execute the main upsert logic
    async fn execute_upsert(
        &self,
        state: Arc<SessionState>,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        // Get unique partition values from source to limit scan scope
        let partition_filters = self.extract_partition_filters().await?;

        // Create target DataFrame with partition filtering
        let target_df = self.create_target_dataframe(&state, &partition_filters)?;

        // Check for conflicts between source and target
        let conflicts_df = self.find_conflicts(&target_df).await?.cache().await?;

        if &conflicts_df.clone().count().await? > &0 {
            self.execute_upsert_with_conflicts(&state, &target_df, &conflicts_df)
                .await
        } else {
            self.execute_simple_append(&state).await
        }
    }

    /// Extract partition values from source to optimize target scanning
    /// This method attempts to identify partition columns from the table schema
    /// and extract unique values from the source data to limit the scan scope
    async fn extract_partition_filters(&self) -> DeltaResult<HashMap<String, Vec<String>>> {
        let mut partition_filters = HashMap::new();
        let partition_columns = &self.snapshot.metadata().partition_columns;

        if partition_columns.is_empty() {
            return Ok(partition_filters);
        }

        for partition_col in partition_columns {
            if let Ok(batches) = self
                .source
                .clone()
                .select(vec![col(partition_col)])? // No clone needed
                .collect()
                .await
            {
                // Collect all values from all batches into a single Vec<String>
                let mut all_values = Vec::new();
                for batch in batches {
                    if let Ok(column_index) = batch.schema().index_of(partition_col) {
                        let column = batch.column(column_index);

                        if let Some(int_array) =
                            column.as_any().downcast_ref::<arrow::array::Int32Array>()
                        {
                            all_values.extend(int_array.iter().flatten().map(|v| v.to_string()));
                        } else if let Some(str_array) =
                            column.as_any().downcast_ref::<arrow::array::StringArray>()
                        {
                            all_values.extend(str_array.iter().flatten().map(|v| v.to_string()));
                        } else if let Some(int64_array) =
                            column.as_any().downcast_ref::<arrow::array::Int64Array>()
                        {
                            all_values.extend(int64_array.iter().flatten().map(|v| v.to_string()));
                        } else if let Some(uint32_array) =
                            column.as_any().downcast_ref::<arrow::array::UInt32Array>()
                        {
                            all_values.extend(uint32_array.iter().flatten().map(|v| v.to_string()));
                        } else if let Some(uint64_array) =
                            column.as_any().downcast_ref::<arrow::array::UInt64Array>()
                        {
                            all_values.extend(uint64_array.iter().flatten().map(|v| v.to_string()));
                        } else {
                            return Err(DeltaTableError::Generic(format!(
                                "Unsupported partition column type for '{}'",
                                partition_col
                            )));
                        }
                    }
                }

                // Deduplicate values across all batches
                let values: Vec<String> = all_values.into_iter().unique().collect();

                if !values.is_empty() {
                    partition_filters.insert(partition_col.to_string(), values);
                }
            }
        }

        Ok(partition_filters)
    }

    /// Create a DataFrame for the target table with partition filtering
    fn create_target_dataframe(
        &self,
        state: &SessionState,
        partition_filters: &HashMap<String, Vec<String>>,
    ) -> DeltaResult<DataFrame> {
        let scan_config = crate::delta_datafusion::DeltaScanConfigBuilder::default()
            .with_file_column_name(&FILE_PATH_COLUMN.to_string())
            .with_parquet_pushdown(true)
            .with_schema(self.snapshot.input_schema().unwrap())
            .build(&self.snapshot)?;

        let target_provider = Arc::new(crate::delta_datafusion::DeltaTableProvider::try_new(
            self.snapshot.clone(),
            self.log_store.clone(),
            scan_config,
        )?);

        // Create partition filters to limit scan scope
        let mut filters = Vec::new();
        for (column, values) in partition_filters {
            if !values.is_empty() {
                let filter_values: Vec<Expr> = values
                    .iter()
                    .map(|v| {
                        // Try to parse as integer first, then as string
                        if let Ok(int_val) = v.parse::<i32>() {
                            lit(int_val)
                        } else if let Ok(int64_val) = v.parse::<i64>() {
                            lit(int64_val)
                        } else {
                            lit(v.clone())
                        }
                    })
                    .collect();

                let filter_expr = Expr::InList(InList {
                    expr: Box::new(col(column)),
                    list: filter_values,
                    negated: false,
                });
                filters.push(filter_expr);
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

    /// Looks for conflicts between source and target data using an inner join on the join keys.
    ///
    /// Returns a `DataFrame` containing only the conflicting target rows, including the file path column.
    ///
    /// **Schema of returned DataFrame:**
    /// - Each join key column (e\.g\. `"workspace_id"`, `"id"`)
    /// - `__delta_rs_path` \(file path of the conflicting row\)
    ///
    /// **Example:**
    /// If join keys are `["workspace_id", "id"]`, the resulting DataFrame schema:
    /// ```
    /// workspace_id: Int32
    /// id: Utf8
    /// __delta_rs_path: Utf8
    /// ```
    /// Each row represents a target record that conflicts with a source record \(i\.e\. same join key values\)\.
    async fn find_conflicts(&self, target_df: &DataFrame) -> DeltaResult<DataFrame> {
        let mut target_keys: Vec<_> = self.join_keys.iter().map(|k| col(k)).collect();
        target_keys.push(col(FILE_PATH_COLUMN));

        let source_keys: Vec<_> = self
            .join_keys
            .iter()
            .map(|k| col(k).alias(&format!("source_{}", k)))
            .collect();

        let target_subset = target_df.clone().select(target_keys)?;
        let source_subset = self.source.clone().select(source_keys)?;

        let source_key_cols: Vec<_> = self
            .join_keys
            .iter()
            .map(|s| format!("source_{}", s))
            .collect();
        let target_key_cols: Vec<_> = self.join_keys.iter().map(|s| format!("{}", s)).collect();

        let conflicts = target_subset
            .join(
                source_subset,
                JoinType::Inner,
                &source_key_cols
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>(),
                &target_key_cols
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>(),
                None,
            )?
            //select only target columns (including file path)
            .select(
                target_key_cols
                    .iter()
                    .map(|s| col(s))
                    .chain(std::iter::once(col(FILE_PATH_COLUMN)))
                    .collect::<Vec<Expr>>(),
            )
            .map_err(|e| {
                DeltaTableError::Generic(format!("Error during conflict detection join: {}", e))
            });

        conflicts
    }

    /// Execute upsert when there are no conflicts - simple append
    async fn execute_simple_append(
        &self,
        state: &SessionState,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        let logical_plan = self.source.clone().into_unoptimized_plan();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        // Get partition columns for writing
        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns.clone();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state.clone(),
            physical_plan,
            partition_columns,
            self.log_store.object_store(None),
            Some(self.snapshot.table_config().target_file_size() as usize),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_config().num_indexed_cols(), None),
            None,
            false,
        )
        .await?;

        let mut metrics = UpsertMetrics::default();

        metrics.num_added_files = add_actions.len();
        metrics.num_removed_files = 0;
        metrics.scan_time_ms = write_metrics.scan_time_ms;
        metrics.write_time_ms = write_metrics.write_time_ms;

        Ok((add_actions, metrics))
    }

    /// Execute upsert when conflicts exist - need to remove old files and write new ones
    async fn execute_upsert_with_conflicts(
        &self,
        state: &SessionState,
        target_df: &DataFrame,
        conflicts_df: &DataFrame,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        let conflicting_file_names = Self::conflicting_filenames(conflicts_df).await?;
        let remove_actions = self.files_to_remove(&conflicting_file_names);
        let filtered_target_df = Self::filter_conflicting_files(target_df, conflicting_file_names)?;
        let non_conflicting_target =
            self.get_non_conflicting_target_rows(&filtered_target_df, &conflicts_df)?;
        let result_df = self.union_source_with_target(&non_conflicting_target)?;

        // Write the combined data
        let logical_plan = result_df.into_unoptimized_plan();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        // Get partition columns for writing
        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns.clone();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state.clone(),
            physical_plan,
            partition_columns,
            self.log_store.object_store(None),
            Some(self.snapshot.table_config().target_file_size() as usize),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_config().num_indexed_cols(), None),
            None,
            false,
        )
        .await?;

        // Store metrics before moving add_actions
        let mut metrics = UpsertMetrics::default();

        metrics.num_added_files = add_actions.len();
        metrics.num_removed_files = remove_actions.len();
        metrics.scan_time_ms = write_metrics.scan_time_ms;
        metrics.write_time_ms = write_metrics.write_time_ms;

        // Combine add and remove actions
        let mut all_actions = add_actions;
        all_actions.extend(remove_actions);

        Ok((all_actions, metrics))
    }

    fn filter_conflicting_files(
        target_df: &DataFrame,
        conflicting_file_names: Vec<String>,
    ) -> Result<DataFrame, DeltaTableError> {
        let filtered_target_df = target_df
            .clone()
            .filter(col(FILE_PATH_COLUMN).in_list(
                conflicting_file_names.iter().map(|p| lit(p)).collect(),
                false,
            ))?
            .drop_columns(&[FILE_PATH_COLUMN])?;
        Ok(filtered_target_df)
    }

    fn files_to_remove(&self, conflicting_file_names: &Vec<String>) -> Vec<Action> {
        let remove_actions = self
            .snapshot
            .eager_snapshot()
            .files()
            .filter(|f| conflicting_file_names.contains(&f.path().to_string()))
            .map(|f| self.logical_file_to_remove(f))
            .collect::<Vec<_>>();
        remove_actions
    }

    async fn conflicting_filenames(
        conflicts_df: &DataFrame,
    ) -> Result<Vec<String>, DeltaTableError> {
        // Find files to remove based on join key values (conflicting files only)
        let conflicting_file_names = conflicts_df
            .clone()
            .select([col(FILE_PATH_COLUMN)])?
            .distinct()?
            .collect()
            .await?
            .iter()
            .flat_map(|batch| {
                let array = batch.column(0);
                if let Some(dict_array) = array
                    .as_any()
                    .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>()
                {
                    let keys = dict_array.keys();
                    let values = dict_array.values();
                    if let Some(str_values) =
                        values.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        return keys
                            .iter()
                            .flatten()
                            .map(|key| str_values.value(key as usize).to_string())
                            .collect::<Vec<String>>();
                    }
                } else if let Some(str_array) =
                    array.as_any().downcast_ref::<arrow::array::StringArray>()
                {
                    return str_array
                        .iter()
                        .flatten()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                }
                Vec::new()
            })
            .collect::<Vec<String>>();
        Ok(conflicting_file_names)
    }

    /// Convert a LogicalFile to an Add action
    fn logical_file_to_remove(&self, f: crate::kernel::LogicalFile) -> Action {
        use delta_kernel::expressions::Scalar;

        // Convert partition values from delta_kernel format to the expected HashMap
        let partition_values = f
            .partition_values()
            .unwrap_or_default()
            .iter()
            .map(|(k, v)| {
                let value = match v {
                    Scalar::Integer(i) => Some(i.to_string()),
                    Scalar::String(s) => Some(s.clone()),
                    Scalar::Long(l) => Some(l.to_string()),
                    _ => None,
                };
                (k.to_string(), value)
            })
            .collect();

        Action::Remove(Remove {
            path: f.path().to_string(),
            data_change: true,
            extended_file_metadata: None,
            size: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
            partition_values: Some(partition_values),
            default_row_commit_version: None,
        })
    }

    /// Get target rows that don't conflict with source (using anti-join)
    fn get_non_conflicting_target_rows(
        &self,
        target_df: &DataFrame,
        conflicts_df: &DataFrame,
    ) -> DeltaResult<DataFrame> {
        // Left anti join: target rows NOT in source (non-conflicting target rows)
        let non_conflicting_target = target_df.clone().join(
            conflicts_df.clone(),
            JoinType::LeftAnti,
            &self
                .join_keys
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            &self
                .join_keys
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            None,
        )?;

        Ok(non_conflicting_target)
    }

    /// Union source data with non-conflicting target rows
    fn union_source_with_target(&self, target_no_conflict: &DataFrame) -> DeltaResult<DataFrame> {
        let result_df = self.source.clone().union(target_no_conflict.clone())?;
        Ok(result_df)
    }

    /// Commit all changes to the Delta log
    async fn commit_changes(
        &self,
        actions: Vec<Action>,
        metrics: &UpsertMetrics,
    ) -> DeltaResult<DeltaTable> {
        // Add metrics to commit metadata
        let mut app_metadata = self.commit_properties.app_metadata.clone();
        app_metadata.insert("readVersion".to_owned(), self.snapshot.version().into());

        if let Ok(metrics_json) = serde_json::to_value(metrics) {
            app_metadata.insert("operationMetrics".to_owned(), metrics_json);
        }

        let mut commit_properties = self.commit_properties.clone();
        commit_properties.app_metadata = app_metadata;

        // Get partition columns for the operation metadata
        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns.clone();

        let operation = crate::protocol::DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns)
            },
            predicate: None,
        };

        let commit = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await?;

        Ok(DeltaTable::new_with_state(
            self.log_store.clone(),
            commit.snapshot(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaOps;
    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use arrow_schema::{ArrowError, DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use delta_kernel::schema::{PrimitiveType, StructField};
    use std::sync::Arc;

    fn create_batch(data: Vec<ArrayRef>) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
                Field::new("workspace_id", DataType::Int32, false),
            ])),
            data,
        )
    }

    async fn setup_test_table() -> DeltaTable {
        let schema = vec![
            StructField::new(
                "id".to_string(),
                delta_kernel::schema::DataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "value".to_string(),
                delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
            StructField::new(
                "workspace_id".to_string(),
                delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
        ];

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(schema)
            .with_partition_columns(["workspace_id"])
            .await
            .unwrap();

        // Add some initial data
        let batch_1 = create_batch(vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 1, 1])),
        ])
        .unwrap();

        // Add some initial data, second batch
        let batch_2 = create_batch(vec![
            Arc::new(StringArray::from(vec!["D"])),
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(Int32Array::from(vec![1])),
        ])
        .unwrap();

        DeltaOps(table.clone()).write([batch_1]).await.unwrap();
        DeltaOps(table.clone()).write([batch_2]).await.unwrap()
    }

    async fn get_table_data(table: DeltaTable) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        let df = ctx.read_table(Arc::new(table)).unwrap();
        df.collect().await.unwrap()
    }

    fn assert_record(data: &Vec<RecordBatch>, expected: (&str, i32)) {
        // Check that the expected record was updated correctly
        let (expected_id, expected_value) = expected;
        let mut found = false;
        for batch in data {
            let id_array = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let value_array = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..batch.num_rows() {
                if id_array.value(i) == expected_id {
                    found = true;
                    assert_eq!(value_array.value(i), expected_value, "Record value mismatch for id '{}'", expected_id);
                }
            }
        }
        assert!(found, "Expected record '{}' not found", expected_id);
    }

    #[tokio::test]
    async fn test_upsert_no_conflicts() {
        let table = setup_test_table().await;

        let source_batch = create_batch(vec![
            Arc::new(StringArray::from(vec!["E", "F"])),
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(Int32Array::from(vec![1, 1])),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch).unwrap();

        let (updated_table, metrics) = DeltaOps(table)
            .upsert(
                source_df,
                vec!["workspace_id".to_string(), "id".to_string()],
            )
            .await
            .unwrap();

        // Should have added files but no removed files since no conflicts
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 0);

        // Should have 6 total rows (4 original + 2 new)
        let data = get_table_data(updated_table).await;
        let total_rows: usize = data.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[tokio::test]
    async fn test_upsert_with_conflicts() {
        let table = setup_test_table().await;

        let source_batch = create_batch(vec![
            Arc::new(StringArray::from(vec!["A", "E"])), // "A" conflicts, "E" doesn't
            Arc::new(Int32Array::from(vec![10, 4])),     // Updated value for A
            Arc::new(Int32Array::from(vec![1, 1])),      // Same workspace as existing A
        ])
            .unwrap();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch).unwrap();

        let (updated_table, metrics) = DeltaOps(table)
            .upsert(
                source_df,
                vec!["workspace_id".to_string(), "id".to_string()],
            )
            .await
            .unwrap();

        // Should have both added and removed files due to conflicts
        assert_eq!(metrics.num_added_files, 2);
        assert_eq!(metrics.num_removed_files, 1);

        // Should still have some rows
        let data = get_table_data(updated_table).await;
        let total_rows: usize = data.iter().map(|batch| batch.num_rows()).sum();

        assert_record(&data, ("A", 10)); // Updated record
        assert_record(&data, ("E", 4));  // New record

        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_upsert_empty_source() {
        let table = setup_test_table().await;

        // Create empty source data
        let source_batch = create_batch(vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch).unwrap();

        let (updated_table, metrics) = DeltaOps(table)
            .upsert(source_df, vec!["id".to_string()])
            .await
            .unwrap();

        // No changes should be made for empty source
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);

        // Original data should remain unchanged
        let data = get_table_data(updated_table).await;
        let total_rows: usize = data.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 4); // Original 4 rows
    }

    #[tokio::test]
    async fn test_upsert_with_another_partition() {
        let table = setup_test_table().await;

        let source_batch = create_batch(vec![
            Arc::new(StringArray::from(vec!["A", "E"])),
            Arc::new(Int32Array::from(vec![1, 4])),
            Arc::new(Int32Array::from(vec![2, 2])), // Different workspace
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch).unwrap();

        let (updated_table, metrics) = DeltaOps(table)
            .upsert(
                source_df,
                vec!["workspace_id".to_string(), "id".to_string()],
            )
            .await
            .unwrap();

        // Should have both added and removed files due to conflicts
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 0);

        // Should still have some rows
        let data = get_table_data(updated_table).await;
        let total_rows: usize = data.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[tokio::test]
    async fn test_upsert_with_custom_properties() {
        let table = setup_test_table().await;

        let source_batch = create_batch(vec![
            Arc::new(StringArray::from(vec!["F"])),
            Arc::new(Int32Array::from(vec![6])),
            Arc::new(Int32Array::from(vec![1])),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch).unwrap();

        let mut commit_props = CommitProperties::default();
        commit_props
            .app_metadata
            .insert("test_key".to_string(), serde_json::json!("test_value"));

        let (updated_table, _) = DeltaOps(table)
            .upsert(
                source_df,
                vec!["workspace_id".to_string(), "id".to_string()],
            )
            .with_commit_properties(commit_props)
            .await
            .unwrap();

        // Verify the commit contains our custom properties
        let history = updated_table.history(None).await.unwrap();
        let latest_commit = &history[0];

        // The operation metrics should be present in the commit
        assert!(latest_commit.operation_parameters.is_some());
    }
}
