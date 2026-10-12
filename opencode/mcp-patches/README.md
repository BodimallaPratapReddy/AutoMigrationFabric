# Native SAP ADT MCP read-only session fix

The fix changes the installed `abap-adt-mcp` 2.7.1 server itself, not a custom SAP source adapter. Policy-enforced read-only destinations use stateless ADT requests. Stateful contexts remain available for mutable destinations. An opaque `Request failed with status code 400` on an authorized read reauthenticates and retries once; semantic SAP errors such as unknown dictionary columns do not retry. A second failure propagates. Source writes remain blocked by the existing policy.

`apply-sap-readonly-session.py PATH_TO_DIST_INDEX_JS` applies the bounded patch, checks its expected code layout, preserves an original backup, and refuses unexpected versions. `test-sap-readonly-session.cjs` exercises native server dispatch for successful recovery, repeated failure, semantic failure, mutable destinations and session modes. Run it with `SAP_MCP_INDEX` pointing to the patched native module.

The user's persistent runtime is `/home/pratap/tools/abap-adt-mcp/runtime/node_modules/abap-adt-mcp`. `startup-mcp.sh` invokes that native entry point with the existing Node executable, settings, authentication and TLS configuration. This avoids losing the fix when an ephemeral npx cache is replaced. Reapply/retest deliberately when upgrading the native server; the patch is not silently applied to new versions.

The agent's earlier DD04L query also used nonexistent REFTABLE/REFFIELD columns. Those references belong in DD03L field metadata. Agent guidance now distinguishes invalid SQL from session failures and preserves SAP diagnostics. Live validation uses the authenticated MCP endpoint, not direct custom SAP adapters.

`apply-sap-include-source.py PATH_TO_DIST_INDEX_JS` corrects the native source reader's overbroad include rule: only OO class includes lose `/source/main`. Function-group and program include source URLs retain it. Bare function-group include URLs that return metadata can fall back to `/source/main`. `test-sap-include-source.cjs` covers explicit and bare function-group includes, program includes and the existing class-include behavior.
