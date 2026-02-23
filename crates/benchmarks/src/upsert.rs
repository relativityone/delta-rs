use std::fmt;

use deltalake_core::operations::upsert::UpsertMetrics;
use deltalake_core::{DeltaResult, DeltaTable};
use deltalake_core::datafusion::prelude::DataFrame;

use crate::merge::MergePerfParams;

/// Join keys used to match records in the web_returns dataset
pub const UPSERT_JOIN_KEYS: &[&str] = &["wr_item_sk", "wr_order_number"];

#[derive(Clone, Copy)]
pub struct UpsertTestCase {
    pub name: &'static str,
    pub params: MergePerfParams,
}

impl fmt::Debug for UpsertTestCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertTestCase")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for UpsertTestCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl UpsertTestCase {
    pub async fn execute(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> DeltaResult<(DeltaTable, UpsertMetrics)> {
        let join_keys = UPSERT_JOIN_KEYS.iter().map(|s| s.to_string()).collect();
        table.upsert(source, join_keys).await
    }
}

const UPSERT_CASES: [UpsertTestCase; 9] = [
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.0_rowsNotMatchedFraction_0.1",
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.1,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.01_rowsNotMatchedFraction_0.1",
        params: MergePerfParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.1,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.1",
        params: MergePerfParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.1,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.5_rowsNotMatchedFraction_0.001",
        params: MergePerfParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.001,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.99_rowsNotMatchedFraction_0.001",
        params: MergePerfParams {
            sample_matched_rows: 0.99,
            sample_not_matched_rows: 0.001,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_1.0_rowsNotMatchedFraction_0.001",
        params: MergePerfParams {
            sample_matched_rows: 1.0,
            sample_not_matched_rows: 0.001,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.0",
        params: MergePerfParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.0,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.01_rowsNotMatchedFraction_0.001",
        params: MergePerfParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.001,
        },
    },
    UpsertTestCase {
        name: "upsert_rowsMatchedFraction_0.5_rowsNotMatchedFraction_0.5",
        params: MergePerfParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.5,
        },
    },
];

pub fn upsert_benchmark_cases() -> &'static [UpsertTestCase] {
    &UPSERT_CASES
}
