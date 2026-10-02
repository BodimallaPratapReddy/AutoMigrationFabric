"""Configured migration choices for each supported source connection type."""

import json
from pathlib import Path

from pydantic import BaseModel


CONFIG_PATH = Path(__file__).resolve().parents[2] / "thirdparty" / "configdb" / "migration_types.json"
SUPPORTED_APPROACHES = {
    "ORACLE_TABLE": "ORACLE",
    "SAP_TABLE": "SAP_ECC",
    "SAP_ODP": "SAP_ECC",
    "SAP_REBUILD": "SAP_ECC",
}


class MigrationTypeOption(BaseModel):
    value: str
    label: str


def load_migration_types() -> dict[str, list[MigrationTypeOption]]:
    """Read deploy-time choices; reject unknown workflows or mismatched sources."""
    raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("migration_types.json must contain an object")
    configured: dict[str, list[MigrationTypeOption]] = {}
    seen: set[str] = set()
    for source_type, options in raw.items():
        if source_type not in set(SUPPORTED_APPROACHES.values()) or not isinstance(options, list):
            raise ValueError(f"Invalid migration source type: {source_type}")
        configured[source_type] = []
        for option in options:
            if not isinstance(option, dict):
                raise ValueError("Migration type options must be objects")
            parsed = MigrationTypeOption.model_validate(option)
            if (SUPPORTED_APPROACHES.get(parsed.value) != source_type
                    or parsed.value in seen or not parsed.label.strip()):
                raise ValueError(f"Invalid migration type option: {parsed.value}")
            configured[source_type].append(parsed)
            seen.add(parsed.value)
    return configured
