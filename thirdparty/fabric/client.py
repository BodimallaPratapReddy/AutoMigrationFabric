"""Synchronous Fabric transport for short-lived Temporal activities."""

import re
import math
from contextlib import AbstractContextManager
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import httpx
from pydantic import ValidationError

from .exceptions import (
    FabricAuthenticationError,
    FabricError,
    FabricJobSubmissionError,
    FabricNotFoundError,
    FabricPermissionError,
    FabricRateLimitError,
    FabricResponseError,
)
from .models import (
    FabricCancellationSubmission,
    FabricJobInstance,
    FabricJobSubmission,
    FabricJobParameter,
    FabricItem,
    FabricConnectionStatus,
    FabricWorkspace,
    Lakehouse,
    normalize_job_status,
)
from .utils import FABRIC_API_URL, connect_to_fabric


def _uuid(value: str, name: str) -> str:
    try:
        return str(UUID(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{name} must be a valid UUID") from exc


def _notebook_job_path(workspace_id: str, notebook_id: str, job_id: str | None = None) -> str:
    path = f"workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/execute/instances"
    return f"{path}/{job_id}" if job_id else path


def _pipeline_job_path(workspace_id: str, pipeline_id: str, job_id: str | None = None) -> str:
    path = f"workspaces/{workspace_id}/dataPipelines/{pipeline_id}/jobs/execute/instances"
    return f"{path}/{job_id}" if job_id else path


def _item_job_path(workspace_id: str, item_id: str, job_id: str) -> str:
    return f"workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_id}"


def _parse_job_location(location: str, *, workspace_id: str, item_id: str) -> str:
    parsed = urlparse(location)
    if (parsed.scheme != "https" or parsed.netloc != "api.fabric.microsoft.com"
            or parsed.query or parsed.fragment):
        raise FabricJobSubmissionError("Fabric submission returned an unexpected Location")
    parts = parsed.path.split("/")
    # Explicit segment matching rejects trailing paths and lookalike prefixes.
    generic = (["", "v1", "workspaces", workspace_id, "items", item_id,
                "jobs", "instances"])
    pipeline = (["", "v1", "workspaces", workspace_id, "dataPipelines", item_id,
                 "jobs", "execute", "instances"])
    if parts[:-1] not in (generic, pipeline):
        raise FabricJobSubmissionError("Fabric submission returned an unexpected Location")
    try:
        return str(UUID(parts[-1]))
    except ValueError as exc:
        raise FabricJobSubmissionError("Fabric submission Location has no job instance ID") from exc


class FabricClient:
    """One authenticated HTTP client reused across related Fabric calls.

    Supply a client for tests or use as a context manager to own the credential.
    Submission POSTs are never retried after transport failures.
    """

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client
        self._owned: AbstractContextManager[httpx.Client] | None = None

    def __enter__(self) -> "FabricClient":
        if self._client is None:
            self._owned = connect_to_fabric()
            self._client = self._owned.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        if self._owned is not None:
            self._owned.__exit__(*args)
            self._owned = None
            self._client = None

    def _request(self, method: str, path: str, *, submission: bool = False, **kwargs: Any) -> httpx.Response:
        if self._client is None:
            raise FabricError("Use FabricClient as a context manager or supply an httpx.Client")
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.RequestError as exc:
            kind = FabricJobSubmissionError if submission else FabricResponseError
            raise kind(f"Fabric {method} request failed: {type(exc).__name__}") from exc
        if response.is_error:
            request_id = (response.headers.get("x-ms-request-id") or
                          response.headers.get("x-ms-correlation-id") or
                          response.headers.get("request-id"))
            retry_after = response.headers.get("Retry-After")
            code = None
            try:
                data = response.json()
                if isinstance(data, dict):
                    error = data.get("error", data)
                    if isinstance(error, dict):
                        code = error.get("errorCode") or error.get("code")
                        request_id = request_id or error.get("requestId")
            except ValueError:
                pass
            # The API's error message may echo a submitted parameter (possibly a secret).
            # Expose stable codes and correlation metadata only.
            detail = f"Fabric {method} returned HTTP {response.status_code}"
            if code:
                detail += f" ({code})"
            if request_id:
                detail += f"; request_id={request_id}"
            if retry_after:
                detail += f"; retry_after={retry_after}"
            kind: type[FabricError] = FabricResponseError
            if response.status_code == 401:
                kind = FabricAuthenticationError
            elif response.status_code == 403:
                kind = FabricPermissionError
            elif response.status_code == 404:
                kind = FabricNotFoundError
            elif response.status_code == 429:
                kind = FabricRateLimitError
            elif submission:
                kind = FabricJobSubmissionError
            retry_seconds = int(retry_after) if retry_after and re.fullmatch(r"\d+", retry_after) else None
            raise kind(detail, status_code=response.status_code, request_id=request_id,
                       retry_after_seconds=retry_seconds, error_code=code)
        return response

    def _json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("expected JSON object")
            return data
        except ValueError as exc:
            raise FabricResponseError("Fabric returned malformed JSON") from exc

    def _pages(self, path: str, model: type[FabricWorkspace] | type[Lakehouse] | type[FabricItem],
               *, params: dict[str, str] | None = None) -> list[Any]:
        values: list[Any] = []
        token: str | None = None
        seen: set[str] = set()
        while True:
            query = {**(params or {}), **({"continuationToken": token} if token else {})}
            response = self._request("GET", path, params=query or None)
            data = self._json(response)
            if not isinstance(data.get("value"), list):
                raise FabricResponseError("Fabric page is missing value list")
            try:
                values.extend(model.model_validate(item) for item in data["value"])
            except ValidationError as exc:
                raise FabricResponseError("Fabric page contains invalid items") from exc
            token = data.get("continuationToken")
            if not token:
                return values
            if not isinstance(token, str) or token in seen:
                raise FabricResponseError("Fabric returned an invalid continuation token")
            seen.add(token)

    def list_workspaces(self) -> list[FabricWorkspace]:
        return self._pages("workspaces", FabricWorkspace)

    def test_connection(self) -> FabricConnectionStatus:
        """Check credentials and workspace access with one bounded GET."""
        response = self._request("GET", "workspaces")
        data = self._json(response)
        if not isinstance(data.get("value"), list):
            raise FabricResponseError("Fabric page is missing value list")
        count = None if data.get("continuationToken") else len(data["value"])
        return FabricConnectionStatus(success=True, accessible_workspace_count=count)

    def get_workspace(self, workspace_id: str) -> FabricWorkspace:
        data = self._json(self._request("GET", f"workspaces/{_uuid(workspace_id, 'workspace_id')}"))
        try:
            return FabricWorkspace.model_validate(data)
        except ValidationError as exc:
            raise FabricResponseError("Fabric returned an invalid workspace") from exc

    def list_lakehouses(self, workspace_id: str) -> list[Lakehouse]:
        return self._pages(f"workspaces/{_uuid(workspace_id, 'workspace_id')}/lakehouses", Lakehouse)

    def get_lakehouse(self, workspace_id: str, lakehouse_id: str) -> Lakehouse:
        path = f"workspaces/{_uuid(workspace_id, 'workspace_id')}/lakehouses/{_uuid(lakehouse_id, 'lakehouse_id')}"
        try:
            return Lakehouse.model_validate(self._json(self._request("GET", path)))
        except ValidationError as exc:
            raise FabricResponseError("Fabric returned an invalid lakehouse") from exc

    def find_lakehouse_by_name(self, workspace_id: str, display_name: str) -> Lakehouse | None:
        matches = [item for item in self.list_lakehouses(workspace_id) if item.display_name == display_name]
        if len(matches) > 1:
            raise FabricResponseError(f"Multiple lakehouses named {display_name!r}; use an ID")
        return matches[0] if matches else None

    def _get_item(self, workspace_id: str, item_id: str, expected_type: str) -> FabricItem:
        path = f"workspaces/{_uuid(workspace_id, 'workspace_id')}/items/{_uuid(item_id, 'item_id')}"
        try:
            item = FabricItem.model_validate(self._json(self._request("GET", path)))
        except ValidationError as exc:
            raise FabricResponseError("Fabric returned an invalid item") from exc
        if item.type != expected_type:
            raise FabricResponseError(f"Fabric item is {item.type}, expected {expected_type}")
        return item

    def get_notebook(self, workspace_id: str, notebook_id: str) -> FabricItem:
        return self._get_item(workspace_id, notebook_id, "Notebook")

    def get_pipeline(self, workspace_id: str, pipeline_id: str) -> FabricItem:
        return self._get_item(workspace_id, pipeline_id, "DataPipeline")

    def list_items(self, workspace_id: str, *, item_type: str | None = None) -> list[FabricItem]:
        if item_type is not None and (not item_type or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", item_type)):
            raise ValueError("item_type must be a Fabric item type")
        path = f"workspaces/{_uuid(workspace_id, 'workspace_id')}/items"
        return self._pages(path, FabricItem, params={"type": item_type} if item_type else None)

    def find_notebook_by_name(self, workspace_id: str, display_name: str) -> FabricItem | None:
        return self._find_item_by_name(workspace_id, display_name, "Notebook")

    def find_pipeline_by_name(self, workspace_id: str, display_name: str) -> FabricItem | None:
        return self._find_item_by_name(workspace_id, display_name, "DataPipeline")

    def _find_item_by_name(self, workspace_id: str, display_name: str, item_type: str) -> FabricItem | None:
        matches = [item for item in self.list_items(workspace_id, item_type=item_type)
                   if item.display_name == display_name]
        if len(matches) > 1:
            raise FabricResponseError(f"Multiple {item_type} items named {display_name!r}; use an ID")
        return matches[0] if matches else None

    def _submission(self, response: httpx.Response, workspace_id: str, item_id: str) -> FabricJobSubmission:
        if response.status_code != 202:
            raise FabricJobSubmissionError(f"Fabric submission returned HTTP {response.status_code}, expected 202")
        location = response.headers.get("Location")
        if not location:
            raise FabricJobSubmissionError("Fabric submission omitted Location header")
        job_id = _parse_job_location(location, workspace_id=workspace_id, item_id=item_id)
        retry = response.headers.get("Retry-After")
        retry_seconds = int(retry) if retry and re.fullmatch(r"\d+", retry) else None
        return FabricJobSubmission(job_instance_id=job_id, location=location, retry_after_seconds=retry_seconds)

    def run_notebook(self, workspace_id: str, notebook_id: str,
                     parameters: dict[str, object] | None = None) -> FabricJobSubmission:
        ws, item = _uuid(workspace_id, "workspace_id"), _uuid(notebook_id, "notebook_id")
        payload = None
        if parameters is not None:
            entries = []
            names: set[str] = set()
            for name, value in parameters.items():
                if not isinstance(name, str) or not name or len(name) > 256:
                    raise ValueError("parameter name must be 1-256 characters")
                if name.casefold() in names:
                    raise ValueError("notebook parameter names must be unique ignoring case")
                names.add(name.casefold())
                if isinstance(value, FabricJobParameter):
                    kind, value = value.type, value.value
                elif isinstance(value, bool):
                    kind = "Boolean"
                elif isinstance(value, (int, float)):
                    kind = "Number"
                elif isinstance(value, str):
                    kind = "Text"
                else:
                    raise ValueError(f"unsupported notebook parameter type for {name!r}")
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError(f"non-finite notebook parameter for {name!r}")
                entries.append({"name": name, "value": value, "type": kind})
            payload = {"parameters": entries}
        response = self._request("POST", _notebook_job_path(ws, item), submission=True, params={"beta": "false"}, json=payload)
        return self._submission(response, ws, item)

    def trigger_provisioning(self, *, workspace_id: str, notebook_id: str, plan_guid: str) -> FabricJobSubmission:
        return self.run_notebook(workspace_id, notebook_id, {"plan_guid": _uuid(plan_guid, "plan_guid")})

    def _parse_job_instance(self, response: httpx.Response, *, item_id: str, job_id: str) -> FabricJobInstance:
        data = self._json(response)
        try:
            status = data["status"]
            if not isinstance(status, str):
                raise ValueError("invalid status")
            properties = data.get("properties")
            if "exitValue" not in data and isinstance(properties, dict):
                data["exitValue"] = properties.get("exitValue")
            retry = response.headers.get("Retry-After")
            retry_seconds = int(retry) if retry and re.fullmatch(r"\d+", retry) else None
            result = FabricJobInstance.model_validate({**data, "normalized_status": normalize_job_status(status),
                                                       "retry_after_seconds": retry_seconds})
            if result.item_id is not None and _uuid(result.item_id, "itemId") != item_id:
                raise ValueError("item ID mismatch")
            if _uuid(result.id, "id") != job_id:
                raise ValueError("job ID mismatch")
            return result
        except (KeyError, ValueError, ValidationError) as exc:
            raise FabricResponseError("Fabric returned an invalid job instance") from exc

    def get_notebook_run(self, workspace_id: str, notebook_id: str, job_instance_id: str) -> FabricJobInstance:
        ws, item, job = (_uuid(workspace_id, "workspace_id"), _uuid(notebook_id, "notebook_id"),
                         _uuid(job_instance_id, "job_instance_id"))
        # Notebook-specific status currently exposes exitValue under properties.
        response = self._request("GET", _notebook_job_path(ws, item, job), params={"beta": "true"})
        return self._parse_job_instance(response, item_id=item, job_id=job)

    def cancel_notebook_run(self, workspace_id: str, notebook_id: str, job_instance_id: str) -> FabricCancellationSubmission:
        ws, item, job = (_uuid(workspace_id, "workspace_id"), _uuid(notebook_id, "notebook_id"),
                         _uuid(job_instance_id, "job_instance_id"))
        response = self._request("POST", _item_job_path(ws, item, job) + "/cancel")
        return self._cancellation(response, workspace_id=ws, item_id=item, job_id=job)

    def run_pipeline(self, workspace_id: str, pipeline_id: str, execution_data: dict[str, object] | None = None) -> FabricJobSubmission:
        ws, item = _uuid(workspace_id, "workspace_id"), _uuid(pipeline_id, "pipeline_id")
        if execution_data is not None:
            raise ValueError("Pipeline execute API does not document an execution_data body")
        response = self._request("POST", _pipeline_job_path(ws, item), submission=True)
        return self._submission(response, ws, item)

    def get_pipeline_run(self, workspace_id: str, pipeline_id: str, job_instance_id: str) -> FabricJobInstance:
        ws, item, job = (_uuid(workspace_id, "workspace_id"), _uuid(pipeline_id, "pipeline_id"),
                         _uuid(job_instance_id, "job_instance_id"))
        response = self._request("GET", _pipeline_job_path(ws, item, job))
        return self._parse_job_instance(response, item_id=item, job_id=job)

    def cancel_pipeline_run(self, workspace_id: str, pipeline_id: str, job_instance_id: str) -> FabricCancellationSubmission:
        ws, item, job = (_uuid(workspace_id, "workspace_id"), _uuid(pipeline_id, "pipeline_id"),
                         _uuid(job_instance_id, "job_instance_id"))
        response = self._request("POST", _item_job_path(ws, item, job) + "/cancel")
        return self._cancellation(response, workspace_id=ws, item_id=item, job_id=job)

    def _cancellation(self, response: httpx.Response, *, workspace_id: str,
                      item_id: str, job_id: str) -> FabricCancellationSubmission:
        if response.status_code != 202:
            raise FabricResponseError(f"Fabric cancellation returned HTTP {response.status_code}, expected 202")
        location = response.headers.get("Location")
        if location:
            try:
                returned_job = _parse_job_location(location, workspace_id=workspace_id, item_id=item_id)
            except FabricJobSubmissionError as exc:
                raise FabricResponseError("Fabric cancellation returned an invalid Location") from exc
            if returned_job != job_id:
                raise FabricResponseError("Fabric cancellation returned a different job instance")
        retry = response.headers.get("Retry-After")
        retry_seconds = int(retry) if retry and re.fullmatch(r"\d+", retry) else None
        return FabricCancellationSubmission(location=location, retry_after_seconds=retry_seconds)
