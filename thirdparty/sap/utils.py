"""Client for SAP HTTP endpoints used by this application."""

from __future__ import annotations

import os
import re
import ssl
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from urllib.parse import quote, urlsplit

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, Field, JsonValue, TypeAdapter


class SAPFieldMetadata(BaseModel):
    fieldname: str
    position: int
    keyflag: str
    rollname: str
    datatype: str
    leng: int
    decimals: int
    checktable: str
    reftable: str
    reffield: str
    ddtext: str


class SAPSQLQueryResponse(BaseModel):
    success: bool
    rows: list[dict[str, JsonValue]]
    row_count: int
    max_rows: int | float
    truncated: bool | None = None


class _SAPDomainValue(StrEnum):
    description: str

    def __new__(cls, code: str, description: str) -> _SAPDomainValue:
        value = str.__new__(cls, code)
        value._value_ = code
        value.description = description
        return value


class SAPDatasourceType(_SAPDomainValue):
    """ROOSTYPE values for an OLTP source."""

    ATTRIBUTES = "ATTR", "Attributes"
    TRANSACTIONAL_DATA = "TRAN", "Transactional Data"
    TEXT = "TEXT", "Text"
    HIERARCHY_NODES = "HIER", "Hierarchy Nodes"
    DATASOURCE_APPEND = "APPE", "DataSource Append"
    ODS_DATASOURCE = "ODS", "ODS DataSource"
    SEGMENTED_DATA = "SEGM", "Segmented Data"


class SAPExtractionMethod(_SAPDomainValue):
    """ROEXMETHOD values for a DataSource extraction method."""

    TABLE_OR_DB_VIEW = "V", "Transparent Table or DB View"
    DOMAIN_FIXED_VALUES = "D", "Fixed Values for Domain"
    FUNCTION_MODULE_COMPLETE = "F1", "Function Module (Complete Interface)"
    FUNCTION_MODULE_SIMPLE = "F2", "Function Module (Simple Interface)"
    FUNCTION_MODULE_SEGMENTED = "FS", "Function Module (Segmented Data Transfer)"
    ABAP_QUERY = "Q", "Extraction Using ABAP Query"
    DATASOURCE_APPEND = "A", "DataSource Append"
    CLASS_BASED_BI_AGENT = "CA", "Class-Based BI Agent"
    ODP_CURSOR_INTERFACE = "CO", "Extraction Using ODP Cursor Interface"


class SAPDatasourceDetails(BaseModel):
    DATASOURCE_ID: str
    OBJVERS: str
    TYPE: SAPDatasourceType
    APPLICATION: str
    EXTRACT_STRUCTURE: str
    EXTRACTOR: str
    EXTRACTION_METHOD: SAPExtractionMethod
    DELTA: str
    DATASOURCE_NAME: str | None


class SAPInfoSetTable(BaseModel):
    tableName: str
    sourceType: str
    usage: str


class SAPInfoSetQueryDetails(BaseModel):
    infoset: str
    sqlTemplate: str | None = None
    sqlTemplateComplete: bool | None = None
    tables: list[SAPInfoSetTable]


class SAPDBViewTable(BaseModel):
    tableName: str
    position: int


class SAPDBViewFilter(BaseModel):
    position: int
    conjunction: str
    tableName: str
    fieldName: str
    negation: str
    operator: str
    value: str
    expression: str


class SAPDBViewQueryDetails(BaseModel):
    view: str
    sqlQuery: str
    sqlQueryComplete: bool
    tables: list[SAPDBViewTable]
    filters: list[SAPDBViewFilter]


class ABAPObjectReference(BaseModel):
    type: str
    name: str


class ABAPScraperSettings(BaseModel):
    recursive: bool
    max_depth: int
    max_objects: int


class ABAPDependency(BaseModel):
    type: str
    name: str
    relation: str
    depth: int
    source_program: str | None = None
    parent_program: str | None = None
    status: str
    reference: bool | None = None
    dependencies: list[ABAPDependency]


class ABAPSourceObject(BaseModel):
    type: str
    name: str
    source_program: str
    parent_program: str
    depth: int
    source_code: str


class ABAPScraperStats(BaseModel):
    objects_returned: int
    objects_visited: int


class ABAPScraperResponse(BaseModel):
    root: ABAPObjectReference
    settings: ABAPScraperSettings
    dependency_tree: ABAPDependency
    objects: list[ABAPSourceObject]
    warnings: list[str]
    stats: ABAPScraperStats
    hit_max_depth: bool | None = None
    hit_max_objects: bool | None = None
    unresolved_count: int | None = None
    complete: bool | None = None


class SAPConnectionStatus(BaseModel):
    success: bool
    sap_client: str


class SAPObjectTypeInfo(BaseModel):
    object_name: str
    object_type: str


class SAPTableSchema(BaseModel):
    table_name: str
    columns: list[SAPFieldMetadata]
    primary_key_columns: list[str]


class SAPDatasourceField(BaseModel):
    datasource: str
    field_name: str
    position: int | None
    selection: str | None = None
    hidden: bool | None = None
    extractable: bool | None = None
    raw_attributes: dict[str, JsonValue] = Field(default_factory=dict)


class SAPODPCapability(BaseModel):
    datasource_name: str
    odp_capable: bool | None
    metadata_found: bool
    context: str | None = None
    raw_attributes: dict[str, JsonValue]


class SAPDeltaDetails(BaseModel):
    datasource_name: str
    delta_supported: bool | None
    delta_indicator: str | None
    delta_mechanism: str | None = None
    details_resolved: bool = False
    raw_attributes: dict[str, JsonValue] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class SAPExtractionRoute(StrEnum):
    TABLE = "TABLE"
    DB_VIEW = "DB_VIEW"
    INFOSET_QUERY = "INFOSET_QUERY"
    FUNCTION_MODULE = "FUNCTION_MODULE"
    CLASS_BASED = "CLASS_BASED"
    DOMAIN_FIXED_VALUES = "DOMAIN_FIXED_VALUES"
    ODP_CURSOR = "ODP_CURSOR"
    DATASOURCE_APPEND = "DATASOURCE_APPEND"
    UNSUPPORTED = "UNSUPPORTED_EXTRACTION_METHOD"


class SAPExtractionResolution(BaseModel):
    route: SAPExtractionRoute
    supported: bool
    reason_code: str | None = None
    reason: str | None = None


class SAPWatermarkCandidate(BaseModel):
    field_name: str
    datatype: str
    indexed: bool | None = None
    nullable: bool | None = None
    reason: str


class SAPDBViewAssessment(BaseModel):
    details: SAPDBViewQueryDetails
    complete: bool
    unresolved_tables: list[str]
    warnings: list[str]


class SAPInfoSetAssessment(BaseModel):
    details: SAPInfoSetQueryDetails
    complete: bool
    unresolved_tables: list[str]
    warnings: list[str]


class SAPObjectInspection(BaseModel):
    object_name: str
    object_type: str
    table_schema: SAPTableSchema | None = None
    view_details: SAPDBViewQueryDetails | None = None
    exists: bool
    warnings: list[str] = Field(default_factory=list)


class ABAPObjectType(StrEnum):
    FUNCTION_MODULE = "FM"


class ABAPScrapeAssessment(BaseModel):
    response: ABAPScraperResponse
    truncated: bool
    possibly_truncated: bool
    complete: bool | None
    unresolved_dependencies: list[ABAPObjectReference]
    warnings: list[str]


def resolve_datasource_extraction(
    details: SAPDatasourceDetails, object_info: SAPObjectTypeInfo | None = None
) -> SAPExtractionRoute:
    method = details.EXTRACTION_METHOD
    if method == SAPExtractionMethod.TABLE_OR_DB_VIEW:
        if object_info is None:
            return SAPExtractionRoute.UNSUPPORTED
        return {
            "TRANSPARENT_TABLE": SAPExtractionRoute.TABLE,
            "DB_VIEW": SAPExtractionRoute.DB_VIEW,
        }.get(object_info.object_type, SAPExtractionRoute.UNSUPPORTED)
    if method in {SAPExtractionMethod.FUNCTION_MODULE_COMPLETE,
                  SAPExtractionMethod.FUNCTION_MODULE_SIMPLE,
                  SAPExtractionMethod.FUNCTION_MODULE_SEGMENTED}:
        return SAPExtractionRoute.FUNCTION_MODULE
    return {
        SAPExtractionMethod.ABAP_QUERY: SAPExtractionRoute.INFOSET_QUERY,
        SAPExtractionMethod.DOMAIN_FIXED_VALUES: SAPExtractionRoute.DOMAIN_FIXED_VALUES,
        SAPExtractionMethod.ODP_CURSOR_INTERFACE: SAPExtractionRoute.ODP_CURSOR,
        # Class traversal has not been verified against the installed SAP endpoint.
        SAPExtractionMethod.CLASS_BASED_BI_AGENT: SAPExtractionRoute.UNSUPPORTED,
    }.get(method, SAPExtractionRoute.UNSUPPORTED)


def resolve_datasource_extraction_details(
    details: SAPDatasourceDetails, object_info: SAPObjectTypeInfo | None = None
) -> SAPExtractionResolution:
    method = details.EXTRACTION_METHOD
    if method == SAPExtractionMethod.DATASOURCE_APPEND:
        return SAPExtractionResolution(route=SAPExtractionRoute.DATASOURCE_APPEND,
                                       supported=False, reason_code="DATASOURCE_APPEND_NOT_SUPPORTED",
                                       reason="DataSource Append requires a separate handler")
    if method == SAPExtractionMethod.CLASS_BASED_BI_AGENT:
        return SAPExtractionResolution(route=SAPExtractionRoute.CLASS_BASED,
                                       supported=False, reason_code="CLASS_BASED_NOT_SUPPORTED",
                                       reason="Class scraper support has not been verified")
    route = resolve_datasource_extraction(details, object_info)
    if route == SAPExtractionRoute.UNSUPPORTED:
        code = "OBJECT_TYPE_UNRESOLVED" if method == SAPExtractionMethod.TABLE_OR_DB_VIEW else "UNKNOWN_EXTRACTION_METHOD"
        return SAPExtractionResolution(route=route, supported=False, reason_code=code,
                                       reason="Extraction route could not be resolved")
    if route in {SAPExtractionRoute.DOMAIN_FIXED_VALUES, SAPExtractionRoute.ODP_CURSOR}:
        code = ("DOMAIN_FIXED_VALUE_REQUIRES_SPECIAL_HANDLER" if route == SAPExtractionRoute.DOMAIN_FIXED_VALUES
                else "ODP_CURSOR_REQUIRES_SPECIAL_HANDLER")
        return SAPExtractionResolution(route=route, supported=False, reason_code=code,
                                       reason="Extraction requires a dedicated handler")
    return SAPExtractionResolution(route=route, supported=True)


def assess_abap_scrape(response: ABAPScraperResponse) -> ABAPScrapeAssessment:
    unresolved: list[ABAPObjectReference] = []
    reached_depth = False

    def visit(node: ABAPDependency) -> None:
        nonlocal reached_depth
        if node.status.upper() not in {"OK", "FOUND", "SUCCESS", "RESOLVED"}:
            unresolved.append(ABAPObjectReference(type=node.type, name=node.name))
        if node.depth >= response.settings.max_depth:
            reached_depth = True
        for child in node.dependencies:
            visit(child)

    visit(response.dependency_tree)
    warnings = list(response.warnings)
    explicit_limit = response.hit_max_depth is True or response.hit_max_objects is True
    heuristic_limit = (response.stats.objects_returned >= response.settings.max_objects
                       or reached_depth or any(re.search(r"truncat|limit|max.depth|max.objects", w, re.I)
                                            for w in warnings))
    truncated = explicit_limit if response.hit_max_depth is not None and response.hit_max_objects is not None else heuristic_limit
    # An endpoint completion flag cannot override evidence of a cut traversal.
    complete = (False if truncated or unresolved else response.complete)
    return ABAPScrapeAssessment(response=response, truncated=truncated,
                                possibly_truncated=truncated,
                                complete=complete,
                                unresolved_dependencies=unresolved, warnings=warnings)


def _sap_name(value: str, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_/$]+", value):
        raise ValueError(f"{label} must be a valid SAP object name")
    return value


_METADATA_ADAPTER = TypeAdapter(list[SAPFieldMetadata])


class SAPClient:
    """Call SAP endpoints with Basic authentication and a configured SAP client.

    Reads SAP_HOST, SAP_HTTPS_PORT, SAP_USER, SAP_PASSWORD, SAP_CLIENT, and
    optional SAP_VERIFY_TLS from the environment or the local .env file.
    """

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        ca_bundle: str | Path | None = None,
        host: str | None = None,
        port: int | str | None = None,
        username: str | None = None,
        password: str | None = None,
        sap_client: str | None = None,
    ) -> None:
        load_dotenv()
        explicit = any(value is not None for value in (host, port, username, password, sap_client))
        if not explicit:
            host = os.getenv("SAP_HOST")
            port = os.getenv("SAP_HTTPS_PORT")
            username = os.getenv("SAP_USER")
            password = os.getenv("SAP_PASSWORD")
            sap_client = os.getenv("SAP_CLIENT")

        missing = [
            name
            for name, value in (
                ("SAP_HOST", host),
                ("SAP_HTTPS_PORT", port),
                ("SAP_USER", username),
                ("SAP_PASSWORD", password),
                ("SAP_CLIENT", sap_client),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing SAP configuration: {', '.join(missing)}")

        port = str(port)
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("SAP_HTTPS_PORT must be a valid TCP port")

        parsed_url = urlsplit(host if "://" in host else f"https://{host}")
        if (
            parsed_url.scheme != "https"
            or not parsed_url.hostname
            or parsed_url.username
            or parsed_url.password
            or parsed_url.path not in ("", "/")
            or parsed_url.query
            or parsed_url.fragment
        ):
            raise ValueError("SAP_HOST must be an HTTPS origin or a bare host")
        if parsed_url.port is not None and parsed_url.port != int(port):
            raise ValueError("SAP_HOST port and SAP_HTTPS_PORT do not match")

        self.sap_client = sap_client
        verify_setting = (os.getenv("SAP_VERIFY_TLS") or "true").strip().lower()
        if verify_setting not in {"true", "false"}:
            raise ValueError("SAP_VERIFY_TLS must be true or false")
        bundle = ca_bundle or os.getenv("SAP_CA_BUNDLE")
        verify = (
            False if verify_setting == "false"
            else ssl.create_default_context(cafile=str(bundle)) if bundle else True
        )
        self._http = httpx.Client(
            base_url=f"https://{parsed_url.hostname}:{port}",
            auth=httpx.BasicAuth(username, password),
            timeout=timeout,
            verify=verify,
        )

    def test_connection(self) -> SAPConnectionStatus:
        # A bounded read of the SAP dictionary verifies both auth and client routing.
        result = self.execute_sql_query("SELECT TABNAME FROM DD02L WHERE TABNAME = 'DD02L'", max_rows=1)
        if not result.success or not result.rows or result.rows[0].get("TABNAME") != "DD02L":
            raise ValueError("SAP dictionary probe did not return DD02L")
        return SAPConnectionStatus(success=True, sap_client=self.sap_client)

    def get_object_type(self, object_name: str) -> SAPObjectTypeInfo:
        name = _sap_name(object_name, "object_name")
        result = self.execute_sql_query(
            "SELECT TABCLASS FROM DD02L "
            f"WHERE TABNAME = '{name}' AND AS4LOCAL = 'A'", max_rows=1)
        tabclass = str(result.rows[0].get("TABCLASS", "")).upper() if result.rows else ""
        object_type = {"TRANSP": "TRANSPARENT_TABLE", "VIEW": "DB_VIEW",
                       "INTTAB": "STRUCTURE"}.get(tabclass, "UNKNOWN")
        return SAPObjectTypeInfo(object_name=name, object_type=object_type)

    def get_table_schema(self, table_name: str) -> SAPTableSchema:
        name = _sap_name(table_name, "table_name")
        columns = self.read_metadata(name)
        return SAPTableSchema(table_name=name, columns=columns,
                              primary_key_columns=[c.fieldname for c in columns
                                                   if c.keyflag.upper() == "X"])

    def object_exists(self, object_name: str) -> bool:
        return self.get_object_type(object_name).object_type != "UNKNOWN"

    def table_exists(self, table_name: str) -> bool:
        return self.get_object_type(table_name).object_type == "TRANSPARENT_TABLE"

    def datasource_exists(self, datasource_name: str) -> bool:
        return self.get_datasource_details(datasource_name) is not None

    def get_watermark_candidates(self, table_name: str) -> list[SAPWatermarkCandidate]:
        if not self.table_exists(table_name):
            raise ValueError(f"{table_name} is not a transparent table")
        candidates = []
        for field in self.get_table_schema(table_name).columns:
            kind = field.datatype.upper()
            if kind in {"DATS", "TIMS", "UTCLONG", "TIMESTAMP"}:
                candidates.append(SAPWatermarkCandidate(
                    field_name=field.fieldname, datatype=kind,
                    reason="Date, time, or timestamp-compatible SAP type"))
        return candidates

    def get_datasource_fields(self, datasource_name: str) -> list[SAPDatasourceField]:
        name = _sap_name(datasource_name, "datasource_name")
        details = self.get_datasource_details(name)
        if details is None:
            raise ValueError(f"DataSource {name} does not exist")
        positions = {field.fieldname: field.position for field in
                     self.read_metadata(details.EXTRACT_STRUCTURE)} if details.EXTRACT_STRUCTURE else {}
        result = self.execute_sql_query(
            "SELECT * FROM ROOSFIELD "
            f"WHERE OLTPSOURCE = '{name}' AND OBJVERS = 'A' ORDER BY FIELD", max_rows=10000)
        if result.truncated is True or (result.truncated is None and result.row_count >= 10000):
            raise ValueError("DataSource field result may be truncated")
        fields = []
        for row in result.rows:
            selection = str(row.get("SELECTION") or "").upper()
            fields.append(SAPDatasourceField(
                datasource=name, field_name=str(row["FIELD"]),
                position=positions.get(str(row["FIELD"])),
                selection=selection or None, raw_attributes=row))
        return fields

    def get_odp_capability(self, datasource_name: str) -> SAPODPCapability:
        name = _sap_name(datasource_name, "datasource_name")
        result = self.execute_sql_query(
            "SELECT EXPOSE_EXTERNAL FROM ROOSATTR "
            f"WHERE OLTPSOURCE = '{name}'", max_rows=1)
        attributes = result.rows[0] if result.rows else {}
        flag = str(attributes.get("EXPOSE_EXTERNAL") or "").strip().upper()
        return SAPODPCapability(datasource_name=name,
                                odp_capable=(flag == "X") if attributes else None,
                                metadata_found=bool(attributes),
                                context="SAPI" if attributes else None, raw_attributes=attributes)

    def get_datasource_delta_details(self, datasource_name: str) -> SAPDeltaDetails:
        name = _sap_name(datasource_name, "datasource_name")
        details = self.get_datasource_details(name)
        if details is None:
            raise ValueError(f"DataSource {name} does not exist")
        indicator = (details.DELTA or "").strip()
        return SAPDeltaDetails(datasource_name=name,
                               delta_supported=False if not indicator else None,
                               delta_indicator=indicator or None,
                               details_resolved=not bool(indicator),
                               raw_attributes={"DELTA": details.DELTA},
                               warnings=["Delta indicator requires SAP-specific interpretation"] if indicator else [])

    def inspect_objects(self, object_names: Sequence[str]) -> dict[str, SAPObjectInspection]:
        inspections = {}
        for name in dict.fromkeys(_sap_name(name, "object_name") for name in object_names):
            kind = self.get_object_type(name).object_type
            schema = self.get_table_schema(name) if kind == "TRANSPARENT_TABLE" else None
            view = self.get_dbview_query_details(name) if kind == "DB_VIEW" else None
            inspections[name] = SAPObjectInspection(
                object_name=name, object_type=kind, table_schema=schema, view_details=view,
                exists=kind != "UNKNOWN")
        return inspections

    def get_objects_metadata(self, object_names: Sequence[str]) -> dict[str, list[SAPFieldMetadata]]:
        names = [_sap_name(name, "object_name") for name in object_names]
        return {name: self.read_metadata(name) for name in dict.fromkeys(names)}

    def read_metadata(self, object_name: str) -> list[SAPFieldMetadata]:
        """Read metadata for a SAP table, view, or structure.

        Return validated field metadata in the endpoint's order. HTTP,
        connection, and response validation errors propagate to callers.
        """
        _sap_name(object_name, "object_name")

        path = f"z_mcp_abap_adt/z_tablemeta/{quote(object_name, safe='')}"
        response = self._http.get(path, params={"sap-client": self.sap_client})
        response.raise_for_status()
        return _METADATA_ADAPTER.validate_python(response.json())

    def execute_sql_query(
        self, sql: str, max_rows: int = 100
    ) -> SAPSQLQueryResponse:
        """Execute trusted application-generated SQL; never expose to user or LLM input."""
        if not sql or not sql.strip():
            raise ValueError("sql must be a nonempty query")
        if type(max_rows) is not int or max_rows < 1:
            raise ValueError("max_rows must be a positive integer")

        response = self._http.post(
            "z_mcp_abap_adt/z_execute_sql",
            params={"sap-client": self.sap_client},
            json={"sql": sql, "max_rows": max_rows},
        )
        response.raise_for_status()
        return SAPSQLQueryResponse.model_validate(response.json())

    def get_datasource_details(
        self, datasource_name: str
    ) -> SAPDatasourceDetails | None:
        """Return active English-language details, or None if not found."""
        if not isinstance(datasource_name, str) or not re.fullmatch(
            r"[A-Za-z0-9_/$]+", datasource_name
        ):
            raise ValueError("datasource_name must be a valid SAP DataSource name")

        sql = (
            "SELECT S.OLTPSOURCE AS DATASOURCE_ID, S.OBJVERS, S.TYPE, "
            "S.APPLNM AS APPLICATION, "
            "S.EXSTRUCT AS EXTRACT_STRUCTURE, S.EXTRACTOR, "
            "S.EXMETHOD AS EXTRACTION_METHOD, S.DELTA, "
            "T.TXTLG AS DATASOURCE_NAME "
            "FROM ROOSOURCE S LEFT JOIN ROOSOURCET T "
            "ON S.OLTPSOURCE = T.OLTPSOURCE AND S.OBJVERS = T.OBJVERS "
            "AND T.LANGU = 'E' "
            f"WHERE S.OLTPSOURCE = '{datasource_name}' "
            "AND S.OBJVERS = 'A'"
        )
        result = self.execute_sql_query(sql, max_rows=100)
        if not result.rows:
            return None
        return SAPDatasourceDetails.model_validate(result.rows[0])

    def get_infoset_query_details(
        self, infoset_query: str
    ) -> SAPInfoSetQueryDetails:
        """Fetch InfoSet source tables and any SQL template the endpoint provides."""
        _sap_name(infoset_query, "infoset_query")

        response = self._http.get(
            "z_mcp_abap_adt/z_infoset",
            params={"sap-client": self.sap_client, "infoset": infoset_query},
        )
        response.raise_for_status()
        return SAPInfoSetQueryDetails.model_validate(response.json())

    def get_dbview_query_details(self, dbview_name: str) -> SAPDBViewQueryDetails:
        """Fetch the SQL query, source tables, and filters for a DB view."""
        _sap_name(dbview_name, "dbview_name")
        response = self._http.get(
            "z_mcp_abap_adt/z_dbview",
            params={"sap-client": self.sap_client, "view": dbview_name},
        )
        response.raise_for_status()
        return SAPDBViewQueryDetails.model_validate(response.json())

    def assess_dbview(self, dbview_name: str) -> SAPDBViewAssessment:
        details = self.get_dbview_query_details(dbview_name)
        unresolved = [table.tableName for table in details.tables
                      if self.get_object_type(table.tableName).object_type not in {"TRANSPARENT_TABLE", "DB_VIEW"}]
        warnings = (["DB view SQL is incomplete"] if not details.sqlQueryComplete else [])
        if unresolved:
            warnings.append("Some source objects could not be verified")
        return SAPDBViewAssessment(details=details, complete=details.sqlQueryComplete and not unresolved,
                                   unresolved_tables=unresolved, warnings=warnings)

    def assess_infoset_query(self, infoset_query: str) -> SAPInfoSetAssessment:
        details = self.get_infoset_query_details(infoset_query)
        unresolved = [table.tableName for table in details.tables
                      if self.get_object_type(table.tableName).object_type not in {"TRANSPARENT_TABLE", "DB_VIEW"}]
        complete = bool(details.sqlTemplate) and details.sqlTemplateComplete is True and not unresolved
        warnings = ([] if details.sqlTemplate and details.sqlTemplateComplete is True
                    else ["InfoSet SQL template is missing or incomplete"])
        if unresolved:
            warnings.append("Some source objects could not be verified")
        return SAPInfoSetAssessment(details=details, complete=complete,
                                    unresolved_tables=unresolved, warnings=warnings)

    def abap_code_scraper(
        self,
        name: str,
        object_type: ABAPObjectType | str = ABAPObjectType.FUNCTION_MODULE,
        *,
        recursive: bool = True,
        max_depth: int = 10,
        max_objects: int = 500,
    ) -> ABAPScraperResponse:
        """Fetch ABAP source and its dependency tree for an SAP object."""
        _sap_name(name, "name")
        try:
            object_type = ABAPObjectType(object_type)
        except ValueError as exc:
            raise ValueError("object_type is not verified for this SAP scraper endpoint") from exc
        if type(recursive) is not bool:
            raise ValueError("recursive must be a boolean")
        if type(max_depth) is not int or max_depth < 0:
            raise ValueError("max_depth must be a nonnegative integer")
        if type(max_objects) is not int or max_objects < 1:
            raise ValueError("max_objects must be a positive integer")

        response = self._http.get(
            "z_mcp_abap_adt/z_abap_scraper",
            params={
                "sap-client": self.sap_client,
                "name": name,
                "type": object_type,
                "recursive": "X" if recursive else "",
                "max_depth": max_depth,
                "max_objects": max_objects,
            },
        )
        response.raise_for_status()
        return ABAPScraperResponse.model_validate(response.json())

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "SAPClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
