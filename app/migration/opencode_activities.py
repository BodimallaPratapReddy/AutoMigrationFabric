"""Bounded OpenCode v2 calls. Credentials stay in the worker environment."""

import hashlib
import json
import os
from urllib.parse import quote

import httpx
from pydantic import ValidationError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .opencode_plan import AgentPlan, adapt_plan


def _client() -> httpx.Client:
    password = os.getenv("OPENCODE_PASSWORD")
    if not password:
        raise ApplicationError("Set OPENCODE_PASSWORD on the worker", non_retryable=True)
    return httpx.Client(base_url=os.getenv("OPENCODE_URL", "http://127.0.0.1:4096"),
        auth=(os.getenv("OPENCODE_USERNAME", "opencode"), password),
        timeout=20, trust_env=False, follow_redirects=False)


def _request(client, method, path, **kwargs):
    try:
        response = client.request(method, path, **kwargs)
        if response.status_code not in {200, 201, 204}:
            raise ApplicationError(f"OpenCode {method} failed (HTTP {response.status_code})",
                                   non_retryable=response.status_code < 500)
        if len(response.content) > 2_000_000:
            raise ApplicationError("OpenCode response exceeds 2 MB", non_retryable=True)
        return response.json() if response.content else {}
    except httpx.HTTPError as exc:
        raise ApplicationError(f"OpenCode {method} transport failed ({type(exc).__name__}); "
                               "check server availability and worker connectivity",
                               type="OpenCodeTransportError") from None


def _path(session_id: str) -> str:
    return "/api/session/" + quote(session_id, safe="")


@activity.defn
def opencode_create(payload: dict) -> dict:
    if not os.getenv("OPENCODE_DIRECTORY"):
        raise ApplicationError("Set OPENCODE_DIRECTORY on the worker", non_retryable=True)
    _prompt(payload)  # Validate the source binding before creating a remote session.
    # Caller-chosen identity allows recovery after a lost create response.
    sid = "ses_" + hashlib.sha256(payload["workflow_id"].encode()).hexdigest()[:26]
    with _client() as client:
        existing = client.get(_path(sid))
        if existing.status_code == 404:
            agent = {'SAP_ODP':os.getenv('OPENCODE_SAP_ODP_AGENT','sap-odp-to-fabric'),
                          'SAP_REBUILD':os.getenv('OPENCODE_SAP_REBUILD_AGENT','sap-datasource-rebuild-to-fabric'),
                          'SAP_VIEW':os.getenv('OPENCODE_SAP_VIEW_AGENT','sap-view-to-fabric'),
                          'SAP_TABLE':os.getenv('OPENCODE_SAP_AGENT','sap-table-to-fabric'),
                          'ORACLE_VIEW':os.getenv('OPENCODE_VIEW_AGENT','oracle-view-to-fabric')}.get(
                              payload['input']['migration_approach'],os.getenv('OPENCODE_AGENT','oracle-to-fabric'))
            catalog = _request(client, "GET", "/api/agent")
            agents = catalog.get("data", [])
            configured = next((item for item in agents if item.get("id") == agent), None)
            if configured is None:
                raise ApplicationError(f"OpenCode agent {agent} is not available", non_retryable=True)
            model = configured.get("model") or {
                "id": os.getenv("OPENCODE_MODEL", "global.anthropic.claude-haiku-5-5"),
                "providerID": os.getenv("OPENCODE_PROVIDER", "amazon-bedrock"),
                "variant": os.getenv("OPENCODE_VARIANT", "medium")}
            _request(client, "POST", "/api/session", json={"id": sid,
                "title": f"Fabric discovery: {payload['input']['source_object_name']}",
                "agent": agent,
                "model": model,
                "location": {"directory": os.environ["OPENCODE_DIRECTORY"]}})
        elif existing.status_code != 200:
            raise ApplicationError(
                f"Cannot reconcile OpenCode session (HTTP {existing.status_code}); "
                "check worker credentials and OpenCode availability",
                non_retryable=existing.status_code < 500 and existing.status_code not in {408, 429})
    return {"session_id": sid}


def _prompt(payload: dict) -> str:
    source = payload["input"]
    if source.get("sap_mcp_discovery"):
        from .sap_mcp_prompt import sap_mcp_prompt
        return sap_mcp_prompt(payload)
    if source['migration_approach'] in {'SAP_TABLE','SAP_VIEW'}:
        from .opencode_sap_plan import SAPAgentPlan
        from thirdparty.sap.extraction import conversion_expression
        is_view = source['migration_approach']=='SAP_VIEW'
        metadata = source.get('source_view_graph') if is_view else source.get('source_sap_metadata')
        if not metadata:
            raise ValueError('Independently verified SAP source metadata is required')
        try:
            bindings = json.loads(os.getenv('OPENCODE_SAP_SOURCE_CONNECTIONS', '{}'))
        except json.JSONDecodeError:
            raise ApplicationError('OPENCODE_SAP_SOURCE_CONNECTIONS must be a JSON object', non_retryable=True) from None
        connection = bindings.get(source['source_connection_name']) if isinstance(bindings, dict) else None
        if not isinstance(connection, str) or not connection.strip():
            raise ApplicationError('Configure OPENCODE_SAP_SOURCE_CONNECTIONS for this source', non_retryable=True)
        if is_view:
            from .opencode_sap_view_plan import SAPViewBundle
            schema = SAPViewBundle.model_json_schema()
            purpose = ('Use sap-to-fabric-view-design and sap-to-fabric-table-design to translate this verified classic SAP database view graph. '
                'Return all base tables once and every nested view in verified dependency order. '
                'Use target_table names prefixed with the root target name and an underscore, for isolated base targets. '
                'The root target_name must match fabric_target_name or source_object_name. '
                'Each spark_sql is one SELECT (optional WITH), no DDL/comments/semicolons, with explicit schema.table references. '
                'Preserve original joins, filters, client semantics, output field names and order. '
                'Translate predicates using the proposed base types: ERDAT is DATE, Boolean flags are BOOLEAN, and long timestamps are exact STRING. '
                'For EACH base table discover all DDIC fields, Data Elements and Domains; no view projection pruning. '
                'The UI reviews columns, keys, watermark and deletes separately for every table, then approves the bundle. ')
        else:
            schema = SAPAgentPlan.model_json_schema()
            purpose = 'Use sap-to-fabric-table-design to propose this transparent SAP table migration. '
        return (purpose +
            'Use sap-abap-adt for actual source metadata, sap-help for SAP business/delta semantics, '
            'abap-docs for ABAP SQL syntax, and microsoft-learn for Lakehouse Spark SQL capabilities. '
            'Documentation lookups are needed only for uncertainties. The supplied source binding and '
            'DDIC snapshot are authoritative. Use only the mapped sap-abap-adt system '
            +json.dumps(connection)+'. Different app/MCP connection labels are expected. '
            'Verify the mapped system host, port and SAP client; hostname/IP aliases may be resolved read-only. '
            'Do not choose another system or ask the user to approve matching labels. '
            'Read the complete user-approved sap-to-fabric-datatype-conversion.md reference, '
            'datatype-mappings.md and extraction-conversions.md from the skill. For EVERY column use '
            'Data Element, then linked Domain, DDIC documentation, physical datatype/length/scale, '
            'and lastly name heuristics. Fetch actual element/domain definitions, documentation and '
            'fixed values on the mapped system, reuse verified lookups, and record per-field semantic '
            'evidence and decision sources in transformation_notes. Cover every datatype in the framework. '
            'Propose canonical STRING, BINARY, BOOLEAN, SMALLINT, INT, BIGINT, DOUBLE, DATE, TIMESTAMP or DECIMAL(p,s). '
            'DATS requires DATE conversion; TIMS requires HH:mm:ss STRING; semantic timestamps require '
            'TIMESTAMP rather than a physical DECIMAL guess. Ordinary DEC/CURR/QUAN keep precision/scale, '
            'integers keep range, FLTP keeps double precision, identifiers/NUMC keep exact strings, '
            'confirmed Boolean domains require a complete value map, and binary needs a verified bytes/Base64 contract. '
            'Do not use legacy app datatype mappings or VARCHAR/VARBINARY(MAX). Do not infer UTC or temporal '
            'meaning from DEC dimensions; preserve initial-value policy, fractional precision and currency/unit references. '
            'Confirmed user policy: DATS exactly 00000000 becomes NULL, and TIMS exactly 000000 becomes '
            '00:00:00 (midnight), never NULL. Do not ask again; the midnight policy supersedes the earlier '
            'tentative zero-time-to-NULL request. Do not extend these approvals to blanks, other malformed '
            'values or zero packed timestamps. Record the deliberate date conversion and external audit requirement. '
            'SourceType must be DATATYPE(LENGTH,DECIMALS) exactly as supplied. Description comes from DDIC only. '
            'The app validates every column/key/watermark against live DDIC and saves the extraction SELECT '
            'from scalar SourceExpression values. The app z_execute_sql endpoint runs native database SQL, '
            'not ABAP Open SQL. Use direct fields, supported native scalar CASTs, or the exact closed '
            'conversion templates supplied below, replacing FIELD with the actual quoted source identifier. '
            'DATS DATE, formatted TIMS STRING, short TIMESTAMP and domain-verified Boolean converters '
            'are now implemented. Use exact STRING with LONG_TIMESTAMP conversion for seven-digit '
            'packed timestamps: this is the approved lossless engineering fallback for this migration, '
            'not Spark TIMESTAMP truncation. Keep original numeric watermark types. '
            'Use strict rejection for other malformed dates/times; do not invent NULL policies. '
            'Do not repeat resolved initial-value, converter or seven-digit precision questions. '
            'Base64 functions outside the templates remain unsupported; direct BINARY requires verified transport. '
            'Templates: '+json.dumps({k:conversion_expression('FIELD',k) for k in
                ('DATE','TIME','TIMESTAMP','LONG_TIMESTAMP','STRING','BOOLEAN')})+'. '
            'The app quotes all source identifiers/aliases and verifies the query with a zero-row read. '
            'Null SourceExpression means direct extraction. Native SQL does not implicitly filter SAP clients; '
            'the external pipeline must explicitly enforce the configured client scope along with watermark bounds. '
            'The UI owns column/key/watermark/delete approvals in later workflow steps. '
            'Never list those pending approvals in unresolved_decisions and do not ask for them here. '
            'Unknown non-key nullability is expected: propose nullable=true; the app preserves it as unknown. '
            'Do not list unknown non-key nullability as unresolved. Ingestion stays disabled until provisioning. '
            'Native question forms collect material unresolved choices; routine mappings should be resolved from evidence. '
            'No DDL/DML, writes, file changes or execution approvals. Read-only metadata calls run sequentially; '
            'do not output credentials or row values. Return exactly one complete fenced JSON object. '
            'Source request: '+json.dumps({k:source.get(k) for k in ('source_connection_name','source_object_name','fabric_schema_name','fabric_target_name')})+
            '. Verified source metadata: '+json.dumps(metadata)+
            '. Schema: '+json.dumps(schema))
    # Explicit binding is required; a registry name is not necessarily a SQLcl name.
    bindings = json.loads(os.getenv("OPENCODE_SOURCE_CONNECTIONS", "{}"))
    connection = bindings.get(source["source_connection_name"])
    if not isinstance(connection, str) or not connection:
        raise ApplicationError("Configure OPENCODE_SOURCE_CONNECTIONS for this source", non_retryable=True)
    numeric_policy = (
        "For unqualified Oracle NUMBER (dictionary precision/scale NULL, sometimes shown as zero placeholders), "
        "follow oracle-to-fabric-table-design's references/number-profiling.md. Run bounded read-only aggregate "
        "profiles and infer business role from comments, constraints, relationships and actual SQL usage. "
        "Autonomously map identifiers/categories without meaningful arithmetic to canonical STRING and "
        "counts/measures to fitting INT, BIGINT or exact DECIMAL(p,s). Preserve numeric ordering and join "
        "compatibility when translating views. Do not ask routine NUMBER type-selection questions. "
        "Record sample scope, fit checks, rationale and future-domain assumptions in the existing evidence, "
        "transformation_notes and warnings fields. Sampling is not a future-range guarantee; never silently "
        "round fractions, overflow or use approximate types. Use exact STRING fallback when evidence is "
        "insufficient; ask only if an unresolved material conflict prevents an exact design. "
        "This current user-approved policy supersedes older ask-for-type guidance; plan approval remains human. ")
    datetime_policy = (
        "Apply oracle-to-fabric-table-design/references/datetime-semantics.md. Oracle DATE and plain "
        "TIMESTAMP are timezone-neutral wall-clock calendar fields: preserve them without any timezone "
        "conversion using pinned UTC Spark evaluation as a representation convention, never a claimed "
        "source timezone. DATE retains time. Do not ask routine source-timezone questions for these types. "
        "Preserve precision; TIMESTAMP(7..9) needs exact STRING fallback. Timezone-aware timestamps need "
        "separate handling. The app permits only one target column per source column: TSTZ UTC plus offset "
        "is not representable here; propose exact canonical STRING retaining offset and named region "
        "where present, or flag a genuine translation blocker. TSLTZ requires verified UTC extraction "
        "with explicit extraction session behavior and has no original offset. Record loader/session "
        "requirements and verified/unverified evidence; do not claim the external loader enforces them. ")
    numeric_policy += datetime_policy
    numeric_policy += (
        "Apply references/blob-handling.md from oracle-to-fabric-table-design to both tables and views. "
        "Classify BLOB business content from evidence. Attachment/document/media BLOBs map to one nullable "
        "STRING target <SOURCE_COLUMN>_FILE_PATH with source_expression CAST(NULL AS VARCHAR2(4000)) "
        "(no alias in source_expression). Retain source name/type and note original nullability. Standard "
        "extraction excludes attachment bytes; a separate attachment process uploads files and populates "
        "paths. This approved deferral is a warning/handoff requirement, not itself an unresolved mapping "
        "decision. Do not invent extra metadata columns or file paths. FULL/REPLACE may reset paths; flag "
        "external loader/attachment coordination. Confirmed small non-attachment binary attributes may use "
        "exact hex only with complete verified BLOB-to-RAW extraction; direct RAWTOHEX(BLOB) is invalid SQL. "
        "Unknown content or BLOB-content operations/presence predicates that cannot be preserved are genuine "
        "blockers. In views reference mapped path columns and retain verified output aliases; never apply "
        "byte functions or BLOB presence filters to initially NULL path strings as if equivalent. ")
    description_policy = (
        "For every base table and column, explicitly read Oracle dictionary comments and return them "
        "unchanged in description, with null for absent comments. Perform the matching standard-table "
        "ETRM lookup and put the detailed table/column description in etrm_description separately; "
        "null if unavailable. Do not substitute inferred meanings, URLs or ETRM text for database comments. "
        "Return both fields explicitly. Fabric table and column comments use only database descriptions. ")
    numeric_policy += description_policy
    if source["migration_approach"] == "ORACLE_VIEW":
        from .opencode_view_plan import AgentViewBundle
        return ("Use oracle-to-fabric-view-design to translate the verified Oracle view graph below. "
            "The app currently accepts STRING, BINARY, BOOLEAN, DATE, TIMESTAMP, TINYINT, SMALLINT, INT, "
            "BIGINT, FLOAT, DOUBLE and DECIMAL(p,s) with 1<=p<=38 and 0<=s<=p. TIMESTAMP_NTZ is unsupported. "
            "Use the authorized wall-clock convention; ask only for genuine remaining semantic blockers. "
            + numeric_policy + "Use ONLY SQLcl connection " + json.dumps(connection) + ". "
            "Produce every base table exactly once and every nested view in dependency order. "
            "Preserve Oracle SQL definitions as evidence. All mappings and Spark SQL must be proposed by you; "
            "the app will not apply legacy datatype mappings. Rewrite references to the selected Fabric schema "
            "using explicit schema.table names (no catalog or Lakehouse prefix). Each view spark_sql must be "
            "one SELECT, optionally with WITH; no CREATE statements, comments or semicolons. "
            "Maintain joins, predicates, nulls, numeric conversion, date semantics and output columns. "
            "Adapt expressions to the actual proposed base column types. Never silently approximate unsupported Oracle logic. "
            "Use forms for material unresolved choices BEFORE returning a final plan. Do not merely list questions "
            "as unresolved_decisions when native questions can collect the answer. Never run DDL/DML, write files/memory, ConfigDB or Fabric. "
            "Small read-only MCP calls must run sequentially. Do not expose source row values or credentials. "
            "Base tables will use FULL/REPLACE, disabled until provisioning; no incremental loading is approved. "
            "Missing comments/business meanings and later replication choices belong in warnings. "
            "Use null descriptions for unknown meanings. Read applicable table-specific memory approvals only. "
            "Return exactly one complete fenced JSON bundle matching the schema, without extra fields. "
            "Destination: " + json.dumps({"workspace": source["fabric_workspace_id"],
                "lakehouse": payload.get("lakehouse_name"), "schema": source["fabric_schema_name"],
                "root_view": source.get("fabric_target_name") or source["source_object_name"]}) +
            ". Verified read-only evidence: " + json.dumps(source["source_view_graph"]) +
            ". Output schema: " + json.dumps(AgentViewBundle.model_json_schema()))
    return ("Discover an Oracle table and propose its Fabric Lakehouse plan. " + numeric_policy + "Use your discovery skill "
        "and live Oracle MCP metadata. Use ONLY SQLcl connection " + json.dumps(connection) +
        " for source " + json.dumps({"schema": source["source_schema_name"],
                                    "table": source["source_object_name"]}) + ". "
        "The user already selected this Fabric destination: " + json.dumps({
            "workspace_id": source["fabric_workspace_id"],
            "lakehouse_id": source["fabric_lakehouse_id"],
            "lakehouse_name": payload.get("lakehouse_name"),
            "schema": source["fabric_schema_name"],
            "table": source.get("fabric_target_name") or source["source_object_name"]}) + ". "
        "Do not ask the user to specify that destination again. "
        "The app separately reviews replication keys, watermark behavior, and delete policy after column review. "
        "Propose watermark candidates; do not require approval of incremental loading during discovery. "
        "Put deferred runtime reviews and unverified watermark update behavior in warnings, not unresolved_decisions. "
        "The runtime starts with ingestion disabled and FULL loading; no incremental method is approved by this plan. "
        "Reserve unresolved_decisions for missing discovery evidence or column mapping choices that prevent column review. "
        "Unknown business descriptions or an absent eTRM entry do not prevent structural column review: "
        "use null descriptions and preserve these limitations in warnings. Never invent business meanings. "
        "Run small read-only metadata queries SEQUENTIALLY, never concurrent SQLcl calls. "
        "Read columns, nullability, keys, indexes, comments and relevant source metadata. "
        "Use native question forms for unresolved business choices. Do not infer approvals from memory "
        "for another table. Do not update files, memory, ConfigDB or Fabric, or run DDL/DML. "
        "Propose all column types and expressions yourself using your skill; the app will not map types. "
        "Return exactly one fenced JSON object conforming to this schema, with no extra fields. "
        "Return canonical uppercase Fabric types. Preserve each source column once. Resolve business questions "
        "using forms before finalizing. If a mapping requires one source column to produce multiple "
        "target columns or unsupported inline extraction, record that unsupported requirement in unresolved_decisions. "
        "An empty unresolved_decisions list "
        "means ready for HUMAN review, not permission to execute. Record missing evidence as unresolved. "
        "Do not put credentials or source row values in the output. Schema: " +
        json.dumps(AgentPlan.model_json_schema()))


@activity.defn
def opencode_prompt(payload: dict) -> None:
    # NO_RETRY at the workflow boundary: an ambiguous POST must be reconciled, not resent.
    mid = "msg_" + hashlib.sha256(
        f"{payload['session_id']}:{payload.get('revision', 0)}".encode()).hexdigest()[:26]
    text = (_prompt(payload) if not payload.get('delivery_continuation') else
        'Continue the approved SAP rebuild multipart delivery in this same session. '
        'All source evidence, datatype rules and reviewer decisions remain unchanged. '
        'Return exactly one next SAP_REBUILD_TABLE_PART or the final SAP_REBUILD_COMPLETE '
        'for bundle_id '+payload['input']['plan_guid']+'. Do not truncate columns or rediscover '
        'completed facts. Delivery parts are not approvals or ConfigDB writes.')
    if payload["input"].get("view_correction"):
        context = payload["input"]["view_correction"]
        if payload['input']['migration_approach']=='SAP_REBUILD':
            from .sap_bundle_storage import expand_bundle
            from .sap_mcp_plan import mapped_destination
            previous=expand_bundle(context['previous'])
            compact_context={k:v for k,v in context.items() if k!='previous'}
            compact_context['previous_views']=previous['review']['agent_plan']['views']
            compact_context['source_view_definitions']=previous['review']['source_view_definitions']
            compact_context['base_schemas']={t['table']['source_table_name']:{
                'target':t['table']['fabric_lakehouse_schema']+'.'+t['table']['fabric_table_name'],
                'columns':{c['target_column_name'] or c['column_name']:c['fabric_data_type'] for c in t['columns']}}
                for t in previous['tables']}
            text += ('\nFor this SAP rebuild correction return ONLY {kind:"SAP_REBUILD_VIEW_REPAIR", '
                'bundle_id:'+json.dumps(payload['input']['plan_guid'])+',destination:'+json.dumps(mapped_destination(payload['input']))+
                ',views:[all prior SAPView entries with only eligible spark_sql/translation_notes corrected]}. '
                'The app supplies the frozen base metadata/plans and rejects any destination, dependency, '
                'output identity or successful-view change. Do not repeat large base-table manifests. '
                'Preserve the approved fail-closed guards. No execution is authorized. Repair context: '+json.dumps(compact_context))
        else:
            text += ("\nThis is a bounded SQL-only notebook repair in the SAME planning session. "
                 "Return the entire JSON bundle. Copy all base table proposals EXACTLY from the previous "
                 "agent_plan; do not reprofile or revise mappings, extraction expressions, table schemas, "
                 "destinations, dependencies or output column identities. Only revise spark_sql and "
                 "translation_notes for failed/blocked eligible views. Preserve successful view SQL. "
                 "Use the structured notebook error and original source definitions to correct the SQL; "
                 "never hide an error by dropping columns or approximating semantics. If a schema change "
                 "is essential, report a blocker using the existing native form instead. No execution is "
                 "authorized by this prompt. Repair context: " + json.dumps(context))
    if payload.get("feedback"):
        text += "\nRevise the existing plan using the reviewer feedback below. Preserve prior table-specific " \
                "answers and verified evidence. Ask native questions for remaining blockers; return the complete " \
                "JSON plan, not an acknowledgement. Reviewer feedback: " + json.dumps(payload["feedback"])
    with _client() as client:
        path = _path(payload["session_id"])
        try:
            # OpenCode can initialize the agent/MCP tools before acknowledging the prompt.
            _request(client, "POST", path + "/prompt", timeout=60,
                     json={"id": mid, "text": text})
        except ApplicationError as original:
            # A lost response is not proof of a rejected prompt. Reconcile the caller-chosen
            # message ID once; never blindly resend a potentially accepted prompt.
            try:
                message = _request(client, "GET", path + "/message/" + mid, timeout=10).get("data", {})
                if message.get("id") == mid and message.get("type") == "user":
                    return
            except (ApplicationError, ValueError):
                pass
            raise original


@activity.defn
def opencode_poll(payload: dict) -> dict:
    path = _path(payload["session_id"])
    with _client() as client:
        messages = None
        extra = {}
        if payload.get('sap_tool_retry_guard'):
            from .sap_tool_retries import tool_attempts
            messages = _request(client, 'GET', path + '/message').get('data', [])
            extra = {'tool_attempts': tool_attempts(messages)}
        forms = _request(client, "GET", path + "/form").get("data", [])
        if forms:
            f = forms[0]
            return {"status": "FORM", "request": {k: f[k] for k in ("id", "title", "fields") if k in f}, **extra}
        permissions = _request(client, "GET", path + "/permission").get("data", [])
        if permissions:
            p = permissions[0]
            return {"status": "PERMISSION", "request": {
                k: p[k] for k in ("id", "action", "resources", "message") if k in p}, **extra}
        if messages is None:
            messages = _request(client, "GET", path + "/message").get("data", [])
    attempts = extra.get('tool_attempts', [])
    # v2 returns newest first. Do not treat an old completed turn as the new result.
    if not messages or messages[0].get("type") != "idle":
        progress = []
        for message in messages:
            if message.get("type") == "user":
                break
            for item in message.get("content", []):
                if item.get("type") == "tool":
                    state = item.get("state", {})
                    calls = state.get("metadata", {}).get("toolCalls", [])
                    for call in calls or [{"tool": item.get("tool") or item.get("name") or "tool",
                                           "status": state.get("status", "running")}]:
                        progress.append({"tool": str(call.get("tool", "tool"))[:120],
                                         "status": str(call.get("status", "running"))[:40]})
        return {"status": "RUNNING", **({"progress": progress[:8]} if progress else {}),
                **({'tool_attempts': attempts} if attempts else {})}
    if messages[0].get("outcome") != "succeeded":
        return {"status": "FAILED", "error": "OpenCode turn failed; inspect the agent session"}
    for message in messages[1:]:
        if message.get("type") == "user":
            break
        if message.get("type") == "assistant" and message.get("finish") == "stop":
            text = "\n".join(c["text"] for c in message.get("content", []) if c.get("type") == "text")
            if len(text.encode()) > 256_000:
                return {"status": "FAILED", "error": "Agent plan exceeds 256 KB"}
            return {"status": "COMPLETED", "text": text,
                    **({'tool_attempts': attempts} if attempts else {})}
    return {"status": "FAILED", "error": "Agent stopped without a final plan"}


@activity.defn
def opencode_reply(payload: dict) -> None:
    kind = payload["kind"]
    route = "form" if kind == "FORM" else "permission"
    body = {"answer": payload["answer"]} if kind == "FORM" else {"decision": payload["decision"]}
    with _client() as client:
        _request(client, "POST", _path(payload["session_id"]) +
                 f"/{route}/{quote(payload['request_id'], safe='')}/reply", json=body)


@activity.defn
def opencode_validate(payload: dict) -> dict:
    try:
        if payload["input"].get("sap_mcp_discovery"):
            from .opencode_sap_view_plan import parse_json
            from .sap_agent_limits import blocked_result
            from .sap_mcp_plan import mapped_destination
            try:
                terminal = parse_json(payload['text'])
            except (ValueError, TypeError):
                terminal = None
            if isinstance(terminal, dict) and terminal.get('kind') == 'SAP_AGENT_BLOCKED':
                return blocked_result(terminal, mapped_destination(payload['input']))
            from .sap_mcp_delivery import handle_rebuild_delivery
            delivery=handle_rebuild_delivery(payload)
            if delivery and 'agent_continuation' in delivery:
                return delivery
            if delivery:
                payload={**payload,'text':delivery['text']}
            from .sap_mcp_plan import adapt_mcp_sap, collect_mcp_evidence
            return adapt_mcp_sap(payload,collect_mcp_evidence(payload["session_id"]))
        if payload['input']['migration_approach']=='SAP_ODP':
            from .opencode_sap_odp_plan import adapt_sap_odp_plan
            result=adapt_sap_odp_plan(payload['text'],payload['input'],payload['lakehouse_name'])
            from .oracle_view_targets import check_oracle_view_targets
            check_oracle_view_targets({'bundle':result,'plan_guid':payload['input']['plan_guid']})
            return result
        if payload['input']['migration_approach']=='SAP_TABLE':
            from .opencode_sap_plan import adapt_sap_plan
            from .source_connections import sap_client
            result = adapt_sap_plan(payload['text'],payload['input'],payload['lakehouse_name'])
            with sap_client(payload['input']['source_connection_name']) as client:
                check = client.execute_sql_query(result['review']['extract_sql'] + ' WHERE 1 = 0', max_rows=1)
            if not check.success or check.rows or check.truncated:
                raise ValueError('SAP extraction SELECT did not pass its zero-row execution check')
            result['review']['extract_sql_validation'] = 'ZERO_ROW_EXECUTION_PASSED'
            return result
        if payload["input"]["migration_approach"] in {"ORACLE_VIEW", "SAP_VIEW", "SAP_REBUILD"}:
            if payload["input"]["migration_approach"] in {"SAP_VIEW","SAP_REBUILD"}:
                from .opencode_sap_view_plan import adapt_sap_view_bundle as adapt_view_bundle
            else:
                from .opencode_view_plan import adapt_view_bundle
            from .oracle_view_targets import check_oracle_view_targets
            result = adapt_view_bundle(payload["text"], payload["input"], payload["lakehouse_name"])
            if payload["input"].get("view_correction"):
                from copy import deepcopy
                from .view_correction import validate_correction
                context = payload["input"]["view_correction"]
                previous = context["previous"]
                original = adapt_view_bundle(json.dumps(previous["review"]["agent_plan"]),
                                             payload["input"], payload["lakehouse_name"])
                if result["tables"] != original["tables"]:
                    raise ValueError("SQL repair cannot change base-table proposals or extraction SQL")
                # The agent schema does not contain the user's replication choices.
                # Restore them only after checking the source/mapping proposal exactly.
                result["tables"] = deepcopy(previous["tables"])
                result["review"]["table_reviews"] = deepcopy(previous["review"]["table_reviews"])
                for field in ("load_method", "write_strategy", "delete_policy", "warnings"):
                    if field in previous["review"]:
                        result["review"][field] = deepcopy(previous["review"][field])
                validate_correction(context["previous"], result, context["eligible"])
            if payload["input"]["migration_approach"] in {"SAP_VIEW","SAP_REBUILD"}:
                from .source_connections import sap_client
                with sap_client(payload["input"]["source_connection_name"]) as client:
                    for review in result["review"]["table_reviews"]:
                        check = client.execute_sql_query(review["extract_sql"] + " WHERE 1 = 0", max_rows=1)
                        if not check.success or check.rows or check.truncated:
                            raise ValueError("SAP base-table extraction did not pass its zero-row execution check")
                        review["extract_sql_validation"] = "ZERO_ROW_EXECUTION_PASSED"
            check_oracle_view_targets({"bundle": result, "plan_guid": payload["input"]["plan_guid"]})
            return result
        result = adapt_plan(payload["text"], payload["input"], payload["lakehouse_name"])
        from .source_connections import oracle_client
        source = payload["input"]
        try:
            oracle = oracle_client(source["source_connection_name"])
            columns = oracle.get_table_schema(source["source_schema_name"], source["source_object_name"])
            description = oracle.get_table_description(source["source_schema_name"], source["source_object_name"])
        except Exception as exc:
            return {"validation_error": f"Oracle database comment verification did not complete ({type(exc).__name__}); retry discovery.",
                    "error_type": "OracleCommentVerificationError"}
        comments = {column.column_name: column.description for column in columns}
        if set(comments) != {c["column_name"] for c in result["table_plan"]["columns"]}:
            raise ValueError("Source columns changed during database comment verification; rediscover the table")
        result["table_plan"]["table"]["description"] = description
        result["review"]["description"] = description
        result["review"]["agent_plan"]["description"] = description
        for entries, key in ((result["table_plan"]["columns"], "column_name"),
                             (result["review"]["columns"], "name"),
                             (result["review"]["agent_plan"]["columns"], "name")):
            for column in entries:
                column["description"] = comments[column[key]]
        return result
    except json.JSONDecodeError as exc:
        return {"validation_error": f"Invalid JSON ({exc.msg}, line {exc.lineno}, column {exc.colno}). Return one complete JSON plan.",
                "error_type": "JSONDecodeError"}
    except ValidationError as exc:
        # Field paths/types only: never echo Pydantic's raw input values.
        details = "; ".join(".".join(map(str, e["loc"])) + ": " + e["type"]
                            for e in exc.errors(include_input=False, include_url=False)[:20])
        return {"validation_error": "Plan schema validation failed: " + details,
                "error_type": "ValidationError"}
    except ValueError as exc:
        return {"validation_error": str(exc)[:2000], "error_type": "ValueError"}
    except (TypeError, KeyError) as exc:
        return {"validation_error": "Plan structure is incomplete; return the required JSON schema.",
                "error_type": type(exc).__name__}


@activity.defn
def opencode_interrupt(payload: dict) -> None:
    with _client() as client:
        _request(client, "POST", _path(payload["session_id"]) + "/interrupt", json={})


ACTIVITIES = [opencode_create, opencode_prompt, opencode_poll, opencode_reply,
              opencode_validate, opencode_interrupt]
