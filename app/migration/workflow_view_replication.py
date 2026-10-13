"""View migration with per-base-table replication approval, retaining SQL repair."""
from copy import deepcopy

from temporalio import workflow

from .workflow_view_correction import OpenCodeOracleViewCorrectionWorkflow


@workflow.defn
class OpenCodeOracleViewReplicationWorkflow(OpenCodeOracleViewCorrectionWorkflow):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await super().run(input)

    async def _discover(self, input: dict, lakehouse_name: str, discovery_activity: str) -> dict:
        result = await super()._discover(input, lakehouse_name, discovery_activity)
        if result.get("failure_reason"):
            return result
        if not input.get("view_correction"):
            self._agent_tables = deepcopy(result["tables"])
        else:
            # Agent JSON describes source metadata and mappings; replication choices
            # belong to the user and are absent from that JSON contract.
            if result["tables"] != self._agent_tables and result["tables"] != self._bundle["tables"]:
                raise ValueError("SQL repair cannot change approved base-table mappings")
            result["tables"] = deepcopy(self._bundle["tables"])
            result["review"]["table_reviews"] = deepcopy(self._bundle["review"]["table_reviews"])
            result["review"].update(load_method="PER_TABLE", write_strategy="PER_TABLE",
                                    delete_policy="PER_TABLE")
            result["review"]["warnings"] = [w for w in result["review"]["warnings"]
                if w != "Base tables use full refresh; incremental loading and delete reconciliation are not enabled."]
        return result

    async def _review_bundle(self, bundle: dict) -> dict:
        original_input = self._input
        bundle_review = bundle["review"]
        self._oracle_key_checks = True
        try:
            for index, (table, review) in enumerate(zip(bundle["tables"], bundle_review["table_reviews"]), 1):
                source = table["table"]
                self._input = {**original_input, "migration_approach": getattr(self,'_source_table_approach','ORACLE_TABLE'),
                    "source_schema_name": source["source_schema_name"],
                    "source_object_name": source["source_table_name"]}
                keys = table["replication"].get("primary_key_columns") or []
                review.update(primary_key=keys, primary_key_source="DATABASE" if keys else None,
                    key_validation_required=True, delete_policy_required=True,
                    view_base_table=True, base_table_index=index, base_table_count=len(bundle["tables"]))
                self.state["review"] = review
                discovered = {"table_plan": table, "review": review}
                while True:
                    mapping = await self._gate("WAITING_FOR_COLUMN_MAPPING_APPROVAL",
                                               ("approve_columns", "reject_columns"))
                    if mapping["action"] == "reject_columns":
                        return {"action": "reject_plan"}
                    try:
                        if not mapping.get("actor"):
                            raise ValueError("Column approval requires a reviewer")
                        self._apply_oracle_column_mappings(discovered, mapping.get("columns") or [], mapping["actor"])
                    except (ValueError, TypeError, KeyError) as exc:
                        review["mapping_error"] = str(exc)
                        continue
                    review.pop("mapping_error", None)
                    choice = await self._watermark_gate(discovered, review_key=True)
                    if choice["action"] == "edit_columns":
                        continue
                    if choice["action"] in {"reject_plan", "reject_watermark"}:
                        return {"action": "reject_plan"}
                    selected = next((c for c in review["watermark_candidates"]
                                     if c["column_name"] == choice.get("watermark_column")), None)
                    replication = table["replication"]
                    if (not choice.get("actor") or choice.get("watermark_column") and
                        (selected is None or not replication.get("primary_key_columns") or
                         (replication.get("key_validation") or {}).get("valid") is not True)):
                        review["watermark_error"] = "Choose a discovered watermark with a verified key, or full refresh"
                        continue
                    indexes = sorted((selected or {}).get("index_details", []),
                                     key=lambda c: (c["column_position"] != 1, c["index_name"]))
                    replication.update(incremental_method="WATERMARK" if selected else "FULL",
                        write_strategy="UPSERT" if selected else "REPLACE",
                        watermark_column=selected["column_name"] if selected else None,
                        watermark_column_data_type=selected["data_type"] if selected else None,
                        watermark_index_name=indexes[0]["index_name"] if indexes else None)
                    review.update(selected_watermark=replication["watermark_column"],
                        selected_watermark_index=replication["watermark_index_name"],
                        load_method=replication["incremental_method"], write_strategy=replication["write_strategy"],
                        watermark_approved_by=choice["actor"])
                    review.pop("watermark_error", None)
                    break
                choice = await self._delete_policy_gate(discovered)
                if choice["action"] == "reject_plan":
                    return choice
        finally:
            self._input = original_input
            self.state["review"] = bundle_review
        bundle_review.update(load_method="PER_TABLE", write_strategy="PER_TABLE", delete_policy="PER_TABLE")
        bundle_review["warnings"] = [w for w in bundle_review["warnings"]
            if w != "Base tables use full refresh; incremental loading and delete reconciliation are not enabled."]
        return await super()._review_bundle(bundle)
