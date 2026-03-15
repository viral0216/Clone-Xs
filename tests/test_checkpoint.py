"""Tests for clone checkpointing."""

import json
import os
import tempfile

from src.checkpoint import CheckpointManager


class TestCheckpointManager:
    def test_record_table(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=100, interval_minutes=60)
        mgr.record_table("schema1", "table1")
        assert "schema1.table1" in mgr._state["completed_tables"]

    def test_auto_save_on_interval(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=2, interval_minutes=60)
        mgr.record_table("s1", "t1")
        mgr.record_table("s1", "t2")
        # Should have auto-saved after 2 tables
        assert os.path.exists(mgr.checkpoint_path)
        # Cleanup
        os.remove(mgr.checkpoint_path)

    def test_save_final(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=100)
        mgr.record_table("s1", "t1")
        mgr.save_final()
        assert os.path.exists(mgr.checkpoint_path)

        # Verify content
        with open(mgr.checkpoint_path) as f:
            data = json.load(f)
        assert "s1.t1" in data["completed_tables"]
        assert "completed_at" in data
        # Cleanup
        os.remove(mgr.checkpoint_path)

    def test_load_checkpoint(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=100)
        mgr.record_table("s1", "t1")
        mgr.record_view("s1", "v1")
        mgr.record_schema_complete("s1")

        state = CheckpointManager.load(mgr.checkpoint_path)
        assert "s1.t1" in state["completed_tables"]
        assert "s1.v1" in state["completed_views"]
        assert "s1" in state["completed_schemas"]
        # Cleanup
        os.remove(mgr.checkpoint_path)

    def test_get_completed_from_checkpoint(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=100)
        mgr.record_table("s1", "t1")
        mgr.record_table("s1", "t2")
        mgr.record_view("s1", "v1")
        mgr.record_schema_complete("s1")

        completed = CheckpointManager.get_completed_from_checkpoint(mgr.checkpoint_path)
        assert ("s1", "t1") in completed["tables"]
        assert ("s1", "t2") in completed["tables"]
        assert ("s1", "v1") in completed["views"]
        assert "s1" in completed["schemas"]
        # Cleanup
        os.remove(mgr.checkpoint_path)

    def test_record_all_types(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        mgr = CheckpointManager(config, interval_tables=100)
        mgr.record_table("s1", "t1")
        mgr.record_view("s1", "v1")
        mgr.record_function("s1", "f1")
        mgr.record_volume("s1", "vol1")
        assert len(mgr._state["completed_tables"]) == 1
        assert len(mgr._state["completed_views"]) == 1
        assert len(mgr._state["completed_functions"]) == 1
        assert len(mgr._state["completed_volumes"]) == 1
