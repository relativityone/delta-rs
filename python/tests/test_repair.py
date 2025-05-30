import os

from deltalake import CommitProperties, DeltaTable, write_deltalake


def test_repair_with_dry_run(tmp_path, sample_table):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")
    dt = DeltaTable(tmp_path)
    os.remove(dt.file_uris()[0])

    metrics = dt.repair(dry_run=True)
    last_action = dt.history(1)[0]

    assert len(metrics["files_removed"]) == 1
    assert metrics["dry_run"] is True
    assert last_action["operation"] == "WRITE"


def test_repair_wo_dry_run(tmp_path, sample_table):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")
    dt = DeltaTable(tmp_path)
    os.remove(dt.file_uris()[0])

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    metrics = dt.repair(dry_run=False, commit_properties=commit_properties)
    last_action = dt.history(1)[0]

    assert len(metrics["files_removed"]) == 1
    assert metrics["dry_run"] is False
    assert last_action["operation"] == "FSCK"
    assert last_action["userName"] == "John Doe"
