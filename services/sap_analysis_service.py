"""Build deterministic SAP DataSource context for later workflow activities."""
from __future__ import annotations

from pydantic import BaseModel, Field

from thirdparty.sap.utils import (
    ABAPScrapeAssessment, SAPClient, SAPDatasourceDetails, SAPDatasourceField,
    SAPDBViewAssessment, SAPExtractionMethod, SAPExtractionResolution,
    SAPExtractionRoute, SAPFieldMetadata, SAPInfoSetAssessment,
    assess_abap_scrape, resolve_datasource_extraction_details,
)


class SAPDatasourceRebuildContext(BaseModel):
    datasource: SAPDatasourceDetails
    extract_structure_schema: list[SAPFieldMetadata]
    datasource_fields: list[SAPDatasourceField]
    extraction_resolution: SAPExtractionResolution
    dbview: SAPDBViewAssessment | None = None
    infoset: SAPInfoSetAssessment | None = None
    abap: ABAPScrapeAssessment | None = None
    warnings: list[str] = Field(default_factory=list)


def build_datasource_rebuild_context(
    client: SAPClient, datasource_name: str
) -> SAPDatasourceRebuildContext:
    details = client.get_datasource_details(datasource_name)
    if details is None:
        raise ValueError(f"DataSource {datasource_name} does not exist")
    schema = client.read_metadata(details.EXTRACT_STRUCTURE) if details.EXTRACT_STRUCTURE else []
    fields = client.get_datasource_fields(datasource_name)
    object_info = (client.get_object_type(details.EXTRACTOR)
                   if details.EXTRACTION_METHOD == SAPExtractionMethod.TABLE_OR_DB_VIEW else None)
    resolution = resolve_datasource_extraction_details(details, object_info)
    dbview = infoset = abap = None
    warnings: list[str] = []
    if resolution.route == SAPExtractionRoute.DB_VIEW:
        dbview = client.assess_dbview(details.EXTRACTOR)
        warnings.extend(dbview.warnings)
    elif resolution.route == SAPExtractionRoute.INFOSET_QUERY:
        infoset = client.assess_infoset_query(details.EXTRACTOR)
        warnings.extend(infoset.warnings)
    elif resolution.route == SAPExtractionRoute.FUNCTION_MODULE:
        abap = assess_abap_scrape(client.abap_code_scraper(details.EXTRACTOR))
        warnings.extend(abap.warnings)
    if not resolution.supported:
        warnings.append(resolution.reason or "Extraction is unsupported")
    return SAPDatasourceRebuildContext(
        datasource=details, extract_structure_schema=schema, datasource_fields=fields,
        extraction_resolution=resolution, dbview=dbview, infoset=infoset,
        abap=abap, warnings=warnings)
