from airflow.sdk import dag, task, Param
import json
from datetime import datetime
import pyarrow as pa
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

"""
TODO:

- Run with data inverval cron schedule
- Add more datasets
- api/main
"""

@dag(
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    max_active_runs=2,
    params={
        'trigger_raw_ingester': Param(False, type='boolean')
        }
    )
def lunch_money():

    @task
    def get_transactions(data_interval_start, data_interval_end, ti):

        from pilot.connections import lunch_money

        strftime = '%Y-%m-%d'

        data = lunch_money.get_transactions(
            date_start=data_interval_start.strftime(strftime),
            date_end=data_interval_end.strftime(strftime)
            ).json()
        
        print(list(data.values()))
        
    
        table = pa.Table.from_pylist(list(data.values())[0])
        schema =[{
            "name":field.name,
            "type":str(field.type)
            }
            for field in table.schema]

        hook = S3Hook()

        key = (
            f'landing/unprocessed/lunch_money/transactions/{data_interval_start.strftime("%Y")}'
            f'/{data_interval_start.strftime("%m")}'
            f'/{data_interval_start.strftime(strftime)}'
            '.json'
            )

        hook.load_string(
        string_data=json.dumps(data['transactions']), 
        key=key,
        bucket_name='warehouse', 
        replace=True 
        )

    prefix = 'landing/unprocessed/lunch_money/transactions/{{ data_interval_start.strftime("%Y") }}'

    @task.branch
    def trigger_raw_ingester(params):

        if params['trigger_raw_ingester']:
            return 'raw_ingester'
        
        return 'downstream_query'

    # Trigger raw_auto_tables
    # Wait for completion
    raw_ingester = TriggerDagRunOperator(
        task_id="raw_ingester",
        trigger_dag_id='raw_ingester',
        trigger_run_id='{{ dag.dag_id }}__{{ ds_nodash }}',
        conf={'prefix': prefix},
        wait_for_completion=True
        )

    # Query raw data into api
    @task(trigger_rule='none_failed')
    def downstream_query():
        pass

    branch = trigger_raw_ingester()
    query = downstream_query()
    get_transactions() >> branch >> raw_ingester >> query
    branch >> query

lunch_money()




