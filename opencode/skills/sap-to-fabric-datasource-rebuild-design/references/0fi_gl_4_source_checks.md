# 0FI_GL_4 verification starting points

These are navigation hints from previous source reads, not approved facts for a new run. Reverify actual product/release, active extractor, referenced object definitions and current configuration through MCP before relying on them. Do not copy the previous table manifest or simplified SQL.

- Read the active ROOSOURCE/ROOSFIELD definition and DTFIGL_4 output fields first.
- BWFID_GET_FIGL_ITEM and BWFIU_CONVERT_FIGL_ITEM lead to BWFIR_READ_DATA, BWFIR_READ_BSEG_CPUDT_DATA, LBWFIUD02 line-item/origin macros, company-code/currency helpers and OPEN_FI_PERFORM_00005021_P.
- On the previously inspected S4CORE 107 / SAP_BASIS 757 system, FINS_T001A had generated SQL view FINSV_T001A. Prefer a verified connector-readable snapshot of that view; do not reconstruct its joins or retain FINSC_001A/FINSC_ACTVE_APPL solely for those joins. FINSC_LD_CMP may still be required independently by native ACDOCA currency amount mapping.
- Follow the actual BSTAT U fallback through FAGL_GET_GL_DOCUMENT, CL_FINS_GET_BSEG mapping/merge, CL_FINS_ACDOC_TRANSFORM_UTIL, relevant field catalogs and TBSL usage. Do not drop supported fallback rows or transcribe only the initial source page.
- Check FI_CURRENCY_INFORMATION STATICS and representative-ledger logic before claiming a first-call approximation is equivalent. The user approved deterministic first-call behavior only under validated safe conditions with consumed fail-closed guards.
- Inspect actual toggle activation (including registry and customizing), exit registrations, ledger uniqueness and mapping catalogs needed for those guards. No SAP generation function may be executed as a Spark guard.
- Delta queues/checkpoints/changed-document tracking are outside rebuilt replication dependencies. A setting remains only for an independently verified business-row effect.
- Use SAP Help for extractor/business semantics, ABAP docs for code behavior and Microsoft Learn for uncertain Spark/Fabric syntax. Resolve technical read gaps directly; authorized metadata/code reads need no extra permission.
- For this test the user approved full refresh for all extraction sources; do not repeat that policy question. CDS/DDIC view keys are not physical uniqueness constraints.
