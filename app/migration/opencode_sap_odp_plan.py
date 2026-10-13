"""ODP output mappings share DDIC validation, but never get table extraction SQL."""
import json
from copy import deepcopy
from .opencode_sap_plan import adapt_sap_plan
from thirdparty.sap.odp_conversion import conversion_contract


def adapt_sap_odp_plan(text,source,lakehouse_name):
    context=source['source_datasource_context']; metadata=context['output_metadata']
    if context['odp_capability']['odp_capable'] is not True:raise ValueError('ODP capability is unverified')
    proposed=adapt_sap_plan(text,{**source,'migration_approach':'SAP_TABLE','source_sap_metadata':metadata},lakehouse_name)
    planned=proposed['table_plan']; review=proposed['review']; delta=context['delta']
    for column,entry in zip(planned['columns'],sorted(review['columns'],key=lambda c:next(f['position'] for f in metadata['schema']['columns'] if f['fieldname']==c['name']))):
        column['source_expression']=conversion_contract(column['source_expression'],column['column_name'])
        column['is_watermark_candidate']=False
        entry['source_expression']=column['source_expression']
    planned['table'].update(source_object_type='SAP_DATASOURCE',data_source_type='ODP',extract_sql=None,
        replication_method='ODP_MANAGED',delta=context['datasource'].get('DELTA'))
    planned['replication'].update(incremental_method='SAP_ODP_DELTA' if delta['details_resolved'] and delta['delta_supported'] is True else 'FULL',
        write_strategy='ODP_MANAGED',delete_policy=None,watermark_column=None,watermark_column_data_type=None)
    review.pop('extract_sql',None);review.pop('extract_sql_dialect',None)
    review.update(kind='sap_odp',load_method=planned['replication']['incremental_method'],write_strategy='ODP_MANAGED',
        datasource=context['datasource'],odp_capability=context['odp_capability'],delta=delta,
        delta_owner='SAP_ODP_EXTRACTOR',delete_owner='SAP_ODP_EXTRACTOR',conversion_stage='ODP_POST_EXTRACT',
        warnings=review['warnings']+['Delta/delete detection is owned by SAP ODP. The external ODP consumer must preserve record modes, ordering and restart/checkpoint semantics; ODP_MANAGED is not a generic APPEND or UPSERT.',
            'SourceExpression contains closed post-extraction conversion JSON, not executable native SQL. Use decode_odp_row or an equivalent typed staging implementation. No ODQ initialization or extraction was executed by this migration.'])
    return dict(tables=[planned],views=[],review=review)
