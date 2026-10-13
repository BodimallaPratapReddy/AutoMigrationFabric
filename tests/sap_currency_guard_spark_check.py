"""Execute approved guard SQL against isolated synthetic Spark fixtures.

Run with pyspark 3.5: python sap_currency_guard_spark_check.py guard.json report.json
No SAP connection, persistent tables, ConfigDB write or Fabric job is used.
"""
import copy
import json
from pathlib import Path
import sys
from pyspark.sql import SparkSession


SCHEMAS={
    'BKPF':'BUKRS STRING, LDGRP STRING, BSTAT STRING',
    'FINSC_LEDGER':'RLDNR STRING, XLEADING STRING',
    'FAGL_TLDGRP_MAP':'LDGRP STRING, RLDNR STRING, REPRESENT STRING',
    'FTGL_CUSTOM_T':'FEATURE_ID STRING','FTGL_CUSTOM':'FEATURE_ID STRING',
    'TPS31':'PROCS STRING','TPS32':'PROCS STRING','TPS34':'PROCS STRING',
    'FINSC_ACDOC_FCT':'SOURCE STRING',
}


def run(guard_path,report_path):
    guard=json.loads(Path(guard_path).read_text(encoding='utf8'))
    sql=guard['spark_guard_sql'].replace('SAP_DATASOURCE_TEST.','__guard_')
    spark=SparkSession.builder.master('local[1]').appName('SAP currency guard synthetic validation').config('spark.ui.enabled','false').getOrCreate()
    spark.sparkContext.setLogLevel('OFF')
    base={name:[] for name in SCHEMAS}
    base.update(BKPF=[('1000','0L','U')],FINSC_LEDGER=[('0L','X')],FAGL_TLDGRP_MAP=[('0L','0L','X')])
    cases=[('safe_first_call',{},None),('safe_no_U_headers',{'BKPF':[]},None),
           ('duplicate_U_headers',{'BKPF':[('1000','0L','U'),('1000','0L','U')]},'G1'),
           ('duplicate_representatives',{'FAGL_TLDGRP_MAP':[('0L','0L','X'),('0L','0L','X')]},'G2'),
           ('missing_representative',{'FAGL_TLDGRP_MAP':[]},'G2'),
           ('nonleading_representative',{'FAGL_TLDGRP_MAP':[('0L','2L','X')]},'G2'),
           ('multiple_leading_ledgers',{'FINSC_LEDGER':[('0L','X'),('2L','X')]},'G2'),
           ('valuation_customizing',{'FTGL_CUSTOM_T':[('FINS_GROUP_VALUATION_LEDGER',)]},'G3'),
           ('customer_exit',{'TPS34':[('00005021',)]},'G4'),
           ('customer_catalogue',{'FINSC_ACDOC_FCT':[('COBL',)]},'G4')]
    results=[]
    try:
        for name,changes,expected in cases:
            fixture=copy.deepcopy(base);fixture.update(changes)
            for table,schema in SCHEMAS.items():
                spark.createDataFrame(fixture[table],schema).createOrReplaceTempView('__guard_'+table)
            # Consume guard_ok in a required predicate while selecting another
            # column: this tests optimizer behavior, not just syntax analysis.
            query=f'SELECT v.id FROM ({sql}) g CROSS JOIN (SELECT 1 AS id) v WHERE g.guard_ok = 1'
            try:
                rows=spark.sql(query).collect()
            except Exception as exc:
                message=str(exc)
                if expected is None or f'FI_CURRENCY_GUARD {expected}' not in message:raise
                outcome='rejected_'+expected
            else:
                if expected is not None or [r.id for r in rows]!=[1]:raise AssertionError('Guard did not enforce '+name)
                outcome='allowed'
            results.append({'case':name,'outcome':outcome})
            print(name,outcome,flush=True)
        Path(report_path).write_text(json.dumps({'spark_version':spark.version,'cases':results,'status':'PASSED'},indent=2),encoding='utf8')
    finally:spark.stop()


if __name__=='__main__':run(sys.argv[1],sys.argv[2])
