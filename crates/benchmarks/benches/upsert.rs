use std::path::PathBuf;

use delta_benchmarks::{prepare_source_and_table, upsert_benchmark_cases, UpsertTestCase};

use divan::{AllocProfiler, Bencher};

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn parquet_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("TPCDS_PARQUET_DIR")
            .unwrap_or_else(|_| "data/tpcds_parquet".to_string()),
    )
}

fn bench_upsert_case(bencher: Bencher, case: &UpsertTestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let parquet_dir = parquet_dir();
            rt.block_on(async move {
                let (source, table) =
                    prepare_source_and_table(&case.params, &tmp_dir, &parquet_dir)
                        .await
                        .expect("prepare inputs");
                (case, source, table, tmp_dir)
            })
        })
        .bench_local_values(|(case, source, table, tmp_dir)| {
            rt.block_on(async move {
                divan::black_box(case.execute_with_upsert(source, table).await.expect("execute upsert"));
            });
            drop(tmp_dir);
        });
}

fn bench_merge_case(bencher: Bencher, case: &UpsertTestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let parquet_dir = parquet_dir();
            rt.block_on(async move {
                let (source, table) =
                    prepare_source_and_table(&case.params, &tmp_dir, &parquet_dir)
                        .await
                        .expect("prepare inputs");
                (case, source, table, tmp_dir)
            })
        })
        .bench_local_values(|(case, source, table, tmp_dir)| {
            rt.block_on(async move {
                divan::black_box(case.execute_with_merge(source, table).await.expect("execute merge"));
            });
            drop(tmp_dir);
        });
}

#[divan::bench(args = upsert_benchmark_cases())]
fn upsert(bencher: Bencher, case: &UpsertTestCase) {
    bench_upsert_case(bencher, case);
}

#[divan::bench(args = upsert_benchmark_cases())]
fn merge(bencher: Bencher, case: &UpsertTestCase) {
    bench_merge_case(bencher, case);
}
