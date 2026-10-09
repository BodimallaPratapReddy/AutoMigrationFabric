"""Validate user-declared replication keys against discovered source columns."""


def validate_primary_key(columns: list[str], available: list[str]) -> list[str]:
    if not isinstance(columns, list) or any(not isinstance(name, str) for name in columns):
        raise ValueError("Primary key fields must be a list of source column names")
    selected = [name.strip() for name in columns]
    if any(not name or name not in available for name in selected):
        raise ValueError("Select primary key fields from the discovered source columns")
    if len(set(selected)) != len(selected):
        raise ValueError("Primary key fields must not be repeated")
    return selected
