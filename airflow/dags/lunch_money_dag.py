from airflow.decorators import dag, task
from source import defaults
import json


@dag(
    schedule='@daily',
    start_date=defaults.datetime(2023, 1, 1),
    catchup=True,
    max_active_runs=2,
    )
def lunch_money():

    @task
    def get_transactions(data_interval_start, data_interval_end):

        from pilot.connections import lunch_money

        strftime = '%Y-%m-%d'

        data = lunch_money.get_transactions(
            date_start=data_interval_start.strftime(strftime),
            date_end=data_interval_end.strftime(strftime)
            )

        hook = defaults.S3Hook()

        key = (
            f'landing/unprocessed/lunch_money/transactions/{data_interval_start.strftime("%Y")}'
            f'/{data_interval_start.strftime("%m")}'
            f'/{data_interval_start.strftime(strftime)}'
            )

        hook.load_string(
        string_data=json.dumps(data.json()), 
        key=key,
        bucket_name='warehouse', 
        replace=True 
        )


    # Trigger raw_auto_tables
    # Wait for completion

    # Query raw data into transactions

    get_transactions()

lunch_money()




