use std::fmt;
use std::path::Path;

use deltalake_core::datafusion::functions::expr_fn;
use deltalake_core::datafusion::{
    logical_expr::{cast, lit},
    prelude::{DataFrame, ParquetReadOptions, SessionContext},
};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::StructField;
use deltalake_core::operations::merge::MergeMetrics;
use deltalake_core::operations::upsert::UpsertMetrics;
use deltalake_core::{arrow, DeltaResult, DeltaTable};
use tempfile::TempDir;
use url::Url;

use crate::merge::{apply_insert_projection, apply_update_projection};

#[derive(Clone, Copy, Debug)]
pub struct UpsertComparisonParams {
    pub sample_matched_rows: f32,
    pub sample_not_matched_rows: f32,
}

#[derive(Clone, Copy)]
pub enum UpsertOperationType {
    /// Native upsert operation
    NativeUpsert,
    /// Traditional merge with when_matched_update and when_not_matched_insert
    MergeUpsert,
}

impl fmt::Debug for UpsertOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpsertOperationType::NativeUpsert => write!(f, "NativeUpsert"),
            UpsertOperationType::MergeUpsert => write!(f, "MergeUpsert"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct UpsertComparisonTestCase {
    pub name: &'static str,
    pub operation_type: UpsertOperationType,
    pub params: UpsertComparisonParams,
}

impl fmt::Debug for UpsertComparisonTestCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertComparisonTestCase")
            .field("name", &self.name)
            .field("operation_type", &self.operation_type)
            .finish()
    }
}

impl UpsertComparisonTestCase {
    pub async fn execute_native_upsert(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> DeltaResult<(DeltaTable, UpsertMetrics)> {
        // Native upsert using the join keys for web_returns
        table
            .upsert(
                source,
                vec!["wr_item_sk".to_string(), "wr_order_number".to_string()],
            )
            .await
    }

    pub async fn execute_merge_upsert(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> DeltaResult<(DeltaTable, MergeMetrics)> {
        // Traditional merge operation with when_matched_update and when_not_matched_insert
        let merge_builder = table
            .merge(
                source,
                "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number",
            )
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(apply_update_projection)?
            .when_not_matched_insert(apply_insert_projection)?;
        
        merge_builder.await
    }

    pub async fn execute(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> DeltaResult<(DeltaTable, ExecutionMetrics)> {
        match self.operation_type {
            UpsertOperationType::NativeUpsert => {
                let (table, metrics) = self.execute_native_upsert(source, table).await?;
                Ok((table, ExecutionMetrics::Upsert(metrics)))
            }
            UpsertOperationType::MergeUpsert => {
                let (table, metrics) = self.execute_merge_upsert(source, table).await?;
                Ok((table, ExecutionMetrics::Merge(metrics)))
            }
        }
    }
}

/// Unified metrics wrapper for comparison
#[derive(Debug)]
pub enum ExecutionMetrics {
    Upsert(UpsertMetrics),
    Merge(MergeMetrics),
}

impl ExecutionMetrics {
    pub fn describe(&self) -> String {
        match self {
            ExecutionMetrics::Upsert(m) => {
                format!(
                    "UpsertMetrics {{ added: {}, removed: {}, conflicts: {}, exec_ms: {} }}",
                    m.num_added_files, m.num_removed_files, m.num_conflicting_records, m.execution_time_ms
                )
            }
            ExecutionMetrics::Merge(m) => {
                format!(
                    "MergeMetrics {{ inserted: {}, updated: {}, deleted: {} }}",
                    m.num_target_rows_inserted, m.num_target_rows_updated, m.num_target_rows_deleted
                )
            }
        }
    }
}

// Test cases covering different scenarios
const UPSERT_COMPARISON_CASES: [UpsertComparisonTestCase; 18] = [
    // Mostly insert scenarios (low match rate)
    UpsertComparisonTestCase {
        name: "native_upsert_low_match_0.01_high_insert_0.5",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.5,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_low_match_0.01_high_insert_0.5",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.5,
        },
    },
    // Mostly update scenarios (high match rate)
    UpsertComparisonTestCase {
        name: "native_upsert_high_match_0.5_low_insert_0.01",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.01,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_high_match_0.5_low_insert_0.01",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.01,
        },
    },
    // Balanced scenarios
    UpsertComparisonTestCase {
        name: "native_upsert_balanced_0.1_0.1",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.1,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_balanced_0.1_0.1",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.1,
        },
    },
    // Very high match rate (almost all updates)
    UpsertComparisonTestCase {
        name: "native_upsert_very_high_match_0.99_tiny_insert_0.001",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.99,
            sample_not_matched_rows: 0.001,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_very_high_match_0.99_tiny_insert_0.001",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.99,
            sample_not_matched_rows: 0.001,
        },
    },
    // High insert scenarios
    UpsertComparisonTestCase {
        name: "native_upsert_no_match_0.0_high_insert_1.0",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 1.0,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_no_match_0.0_high_insert_1.0",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 1.0,
        },
    },
    // Medium scenarios
    UpsertComparisonTestCase {
        name: "native_upsert_medium_match_0.25_medium_insert_0.25",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.25,
            sample_not_matched_rows: 0.25,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_medium_match_0.25_medium_insert_0.25",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.25,
            sample_not_matched_rows: 0.25,
        },
    },
    // Edge case: only updates, no inserts
    UpsertComparisonTestCase {
        name: "native_upsert_only_updates_0.5_no_insert_0.0",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.0,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_only_updates_0.5_no_insert_0.0",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.0,
        },
    },
    // Small workload
    UpsertComparisonTestCase {
        name: "native_upsert_small_0.05_0.05",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.05,
            sample_not_matched_rows: 0.05,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_small_0.05_0.05",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.05,
            sample_not_matched_rows: 0.05,
        },
    },
    // Large workload
    UpsertComparisonTestCase {
        name: "native_upsert_large_0.75_0.25",
        operation_type: UpsertOperationType::NativeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.75,
            sample_not_matched_rows: 0.25,
        },
    },
    UpsertComparisonTestCase {
        name: "merge_upsert_large_0.75_0.25",
        operation_type: UpsertOperationType::MergeUpsert,
        params: UpsertComparisonParams {
            sample_matched_rows: 0.75,
            sample_not_matched_rows: 0.25,
        },
    },
];

pub fn upsert_comparison_cases() -> &'static [UpsertComparisonTestCase] {
    &UPSERT_COMPARISON_CASES
}

pub fn upsert_comparison_case_names() -> Vec<&'static str> {
    UPSERT_COMPARISON_CASES.iter().map(|c| c.name).collect()
}

pub fn upsert_comparison_case_by_name(name: &str) -> Option<&'static UpsertComparisonTestCase> {
    UPSERT_COMPARISON_CASES
        .iter()
        .find(|case| case.name.eq_ignore_ascii_case(name))
}

pub async fn prepare_source_and_table(
    params: &UpsertComparisonParams,
    tmp_dir: &TempDir,
    parquet_dir: &Path,
) -> DeltaResult<(DataFrame, DeltaTable)> {
    let ctx = SessionContext::new();

    let parquet_path = parquet_dir
        .join("web_returns.parquet")
        .to_str()
        .unwrap()
        .to_owned();

    let parquet_df = ctx
        .read_parquet(&parquet_path, ParquetReadOptions::default())
        .await?;
    let temp_table_url = Url::from_directory_path(tmp_dir).unwrap();

    let schema = parquet_df.schema();
    let delta_schema: deltalake_core::kernel::StructType = schema.as_arrow().try_into_kernel().unwrap();

    let batches = parquet_df.collect().await?;
    let fields: Vec<StructField> = delta_schema.fields().cloned().collect();
    let table = DeltaTable::try_from_url(temp_table_url)
        .await?
        .create()
        .with_columns(fields)
        .await?;

    let table = table.write(batches).await?;

    let source = ctx
        .read_parquet(&parquet_path, ParquetReadOptions::default())
        .await?;

    let matched = source
        .clone()
        .filter(expr_fn::random().lt_eq(lit(params.sample_matched_rows)))?;

    let rand = cast(
        expr_fn::random() * lit(u32::MAX),
        arrow::datatypes::DataType::Int64,
    );
    let not_matched = source
        .filter(expr_fn::random().lt_eq(lit(params.sample_not_matched_rows)))?
        .with_column("wr_item_sk", rand.clone())?
        .with_column("wr_order_number", rand)?;

    let source = matched.union(not_matched)?;
    Ok((source, table))
}
