# Benchmarks

The merge benchmarks are similar to the ones used by [Delta Spark](https://github.com/delta-io/delta/pull/1835).


## Dataset

To generate the database, `duckdb` can be used. Install `duckdb` by following [these instructions](https://duckdb.org/#quickinstall).

Run the following commands:

```bash
❯ duckdb
D CALL dsdgen(sf = 1);
100% ▕██████████████████████████████████████▏ (00:00:05.76 elapsed)
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ 0 rows  │
└─────────┘
D EXPORT DATABASE 'tpcds_parquet' (FORMAT PARQUET);
```

This will generate a folder called `tpcds_parquet` containing many parquet files. Place it at `crates/benchmarks/data/tpcds_parquet` (or set `TPCDS_PARQUET_DIR`). Credits to [Xuanwo's Blog](https://xuanwo.io/links/2025/02/duckdb-is-the-best-tpc-data-generator/).

## Running benchmarks

Benchmarks use Divan and time only the merge operation. A temporary Delta table is created per iteration from `web_returns.parquet` and removed afterwards.

Environment variables:
- `TPCDS_PARQUET_DIR` (optional): directory containing `web_returns.parquet`. Default: `crates/benchmarks/data/tpcds_parquet`.

### Merge benchmarks

From the repo root:
```
cargo bench -p delta-benchmarks --bench merge
```

Filter a specific suite:
```
cargo bench -p delta-benchmarks --bench merge -- delete_only
cargo bench -p delta-benchmarks --bench merge -- multiple_insert_only
cargo bench -p delta-benchmarks --bench merge -- upsert_file_matched
```

### Upsert vs Merge comparison benchmarks

This benchmark suite compares the performance of the native `upsert` operation against the traditional `merge` operation with `when_matched_update` and `when_not_matched_insert`. Both operations are functionally equivalent, implementing an upsert pattern.

From the repo root:
```
cargo bench -p delta-benchmarks --bench upsert_vs_merge
```

Filter specific scenarios:
```
# Compare native upsert vs merge for low match rate scenarios
cargo bench -p delta-benchmarks --bench upsert_vs_merge -- low_match

# Compare for high match rate scenarios
cargo bench -p delta-benchmarks --bench upsert_vs_merge -- high_match

# Compare balanced scenarios
cargo bench -p delta-benchmarks --bench upsert_vs_merge -- balanced
```

The benchmark suite includes 9 different scenarios, each tested with both operations:
- Low match rate (mostly inserts)
- High match rate (mostly updates)
- Balanced scenarios
- Edge cases (only updates, only inserts)
- Small and large workloads

Each scenario uses substantial datasets from the TPC-DS benchmark with varying match and insert fractions to exercise different code paths.

## Profiling script

A simple CLI is available to run a single merge with configurable parameters (useful for profiling or ad-hoc runs). It creates a fresh temporary Delta table per sample from `web_returns.parquet`, times only the merge, and prints duration and metrics.

Run (from repo root):
```bash
cargo run --profile profiling -p delta-benchmarks -- merge --op upsert --matched 0.01 --not-matched 0.10
```

Options:
- `--op <upsert|delete|insert>`: operation to benchmark
- `--matched <fraction>`: fraction of rows that match existing keys (default 0.01)
- `--not-matched <fraction>`: fraction of rows that do not match (default 0.10)
- `--case <name>`: run one of the predefined merge scenarios mirrored from the Delta Spark suite

List cases with:
```bash
cargo run --release -p delta-benchmarks -- merge --case single_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.05
```

### Flamegraphs using `samply`

Using `samply`, you can generate flamegraphs from the profile script.

To start,

```bash
cargo install samply --locked
cargo build --profile profiling -p delta-benchmarks
samply record ./target/profiling/delta-benchmarks upsert
```
