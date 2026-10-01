"""Verify model-identified SAP objects before they enter a runtime plan."""
from __future__ import annotations

import httpx
from pydantic import BaseModel, Field

from .utils import SAPClient, SAPDBViewQueryDetails, SAPFieldMetadata


class SAPObjectCandidate(BaseModel):
    name: str = Field(min_length=1)
    object_type: str
    evidence: list[str] = Field(min_length=1)


class VerifiedSAPObject(BaseModel):
    name: str
    object_type: str
    fields: list[SAPFieldMetadata]
    primary_key_columns: list[str] = Field(default_factory=list)
    view_details: SAPDBViewQueryDetails | None = None
    source_objects: list[str] = Field(default_factory=list)


class UnresolvedSAPObject(BaseModel):
    candidate: SAPObjectCandidate
    reason_code: str
    reason: str


class SAPObjectVerification(BaseModel):
    verified: list[VerifiedSAPObject]
    unresolved: list[UnresolvedSAPObject]
    warnings: list[str]


_TYPE_ALIASES = {
    "TABLE": "TRANSPARENT_TABLE", "SAP_TABLE": "TRANSPARENT_TABLE",
    "TRANSPARENT_TABLE": "TRANSPARENT_TABLE", "VIEW": "DB_VIEW", "DB_VIEW": "DB_VIEW",
}


def normalize_candidate_object_type(value: str) -> str | None:
    return _TYPE_ALIASES.get(value.strip().upper())


def verify_llm_identified_sap_objects(
    client: SAPClient, candidates: list[SAPObjectCandidate]
) -> SAPObjectVerification:
    verified: list[VerifiedSAPObject] = []
    unresolved: list[UnresolvedSAPObject] = []
    warnings: list[str] = []
    seen: set[tuple[str | None, str]] = set()
    active: set[str] = set()
    done: set[str] = set()

    def reject(candidate: SAPObjectCandidate, code: str, reason: str) -> None:
        unresolved.append(UnresolvedSAPObject(candidate=candidate, reason_code=code, reason=reason))
        warnings.append(f"{candidate.name}: {reason}")

    def inspect(candidate: SAPObjectCandidate, expected: str | None) -> bool:
        name = candidate.name.upper()
        if name in done:
            return True
        if name in active:
            return True
        if expected is None:
            reject(candidate, "UNSUPPORTED_OBJECT_TYPE", "Unsupported candidate object type")
            return False
        active.add(name)
        try:
            try:
                kind = client.get_object_type(name).object_type
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                reject(candidate, "NOT_FOUND", "SAP dictionary object was not found")
                return False
            except Exception as exc:
                reject(candidate, "METADATA_READ_FAILED", f"Dictionary lookup failed: {type(exc).__name__}")
                return False
            if kind == "UNKNOWN":
                reject(candidate, "NOT_FOUND", "SAP dictionary object was not found")
                return False
            if kind not in {"TRANSPARENT_TABLE", "DB_VIEW"}:
                reject(candidate, "UNSUPPORTED_OBJECT_TYPE", f"Unsupported SAP dictionary type: {kind}")
                return False
            if kind != expected:
                reject(candidate, "MODEL_TYPE_MISMATCH", f"Model type {expected} differs from SAP type {kind}")
                return False
            try:
                fields = client.read_metadata(name)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                reject(candidate, "NOT_FOUND", "SAP field metadata was not found")
                return False
            except Exception as exc:
                reject(candidate, "METADATA_READ_FAILED", f"Field metadata failed: {type(exc).__name__}")
                return False
            view = None
            sources: list[str] = []
            if kind == "DB_VIEW":
                try:
                    view = client.get_dbview_query_details(name)
                except Exception as exc:
                    reject(candidate, "VIEW_DETAILS_FAILED", f"View details failed: {type(exc).__name__}")
                    return False
                if not view.sqlQueryComplete:
                    reject(candidate, "VIEW_DETAILS_FAILED", "View SQL is incomplete")
                    return False
                for source in view.tables:
                    source_name = source.tableName.upper()
                    if source_name not in sources:
                        sources.append(source_name)
                    try:
                        source_type = client.get_object_type(source_name).object_type
                    except Exception as exc:
                        reject(SAPObjectCandidate(name=source_name, object_type="UNKNOWN", evidence=[name]),
                               "METADATA_READ_FAILED", f"View source lookup failed: {type(exc).__name__}")
                        return False
                    if source_type not in {"TRANSPARENT_TABLE", "DB_VIEW"}:
                        reject(SAPObjectCandidate(name=source_name, object_type="UNKNOWN", evidence=[name]),
                               "NOT_FOUND" if source_type == "UNKNOWN" else "UNSUPPORTED_OBJECT_TYPE",
                               "View source cannot be verified")
                        return False
                    nested = SAPObjectCandidate(name=source_name, object_type=source_type, evidence=[name])
                    if not inspect(nested, source_type):
                        return False
            verified.append(VerifiedSAPObject(
                name=name, object_type=kind, fields=fields,
                primary_key_columns=[field.fieldname for field in fields if field.keyflag.upper() == "X"],
                view_details=view, source_objects=sources))
            done.add(name)
            return True
        finally:
            active.discard(name)

    for candidate in candidates:
        expected = normalize_candidate_object_type(candidate.object_type)
        key = (expected, candidate.name.upper())
        if key in seen:
            continue
        seen.add(key)
        inspect(candidate, expected)
    return SAPObjectVerification(verified=verified, unresolved=unresolved, warnings=warnings)
