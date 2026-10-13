"""Analyze proposed Spark view SQL against isolated, empty mapped schemas.

Input: JSON {schemas:{target:[{name,type}]}, views:[SAPView...],output_columns:[]}.
This validates SQL and inferred output types, not source row equivalence.
"""
import json
from pathlib import Path
import sys
from pyspark.sql import SparkSession


def run(input_path,report_path):
    plan=json.loads(Path(input_path).read_text(encoding='utf8'))
    spark=SparkSession.builder.master('local[1]').appName('SAP rebuild isolated SQL analysis').config('spark.ui.enabled','false').getOrCreate()
    spark.sparkContext.setLogLevel('OFF')
    try:
        for name,columns in plan['schemas'].items():
            ddl=', '.join('`'+c['name']+'` '+c['type'] for c in columns)
            spark.createDataFrame([],ddl).createOrReplaceTempView('__plan_'+name)
        result={}
        for view in plan['views']:
            # Test plans use this explicitly selected namespace; no persistent
            # objects are accessed or changed by this isolated session.
            sql=view['spark_sql'].replace('SAP_DATASOURCE_TEST.','__plan_')
            frame=spark.sql(sql)
            result[view['target_name']]=[{'name':f.name,'type':f.dataType.simpleString()} for f in frame.schema.fields]
            frame.createOrReplaceTempView('__plan_'+view['target_name'])
        final=result[plan['views'][-1]['target_name']]
        if [c['name'] for c in final]!=plan['output_columns']:raise AssertionError('Output columns differ from the approved order')
        Path(report_path).write_text(json.dumps({'spark_version':spark.version,'status':'PASSED','views':result},indent=2),encoding='utf8')
        print('Spark analysis passed; output names/order match. Types recorded.',flush=True)
    except Exception as exc:
        code=exc.getErrorClass() if hasattr(exc,'getErrorClass') else type(exc).__name__
        Path(report_path).write_text(json.dumps({'spark_version':spark.version,'status':'FAILED','error_class':code,'message':str(exc)[:2500]},indent=2),encoding='utf8')
        print('Spark analysis failed:',code,str(exc)[:600],flush=True)
        raise
    finally:spark.stop()


if __name__=='__main__':run(sys.argv[1],sys.argv[2])
