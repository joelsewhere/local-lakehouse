from airflow import DAG
from airflow.sdk import task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(dag_id="test_database") as dag:

    final_op = SQLExecuteQueryOperator(
                task_id="join_data",
                conn_id="trino_main",
                sql="""
                SELECT *
                FROM raw.example.test r
                JOIN api.example.test a
                    ON r.greeting = a.greeting
                JOIN main.example.test m
                    ON r.greeting = m.greeting
"""
                )


    for database in ['raw', 'api', 'main']:

        @task_group(group_id=database)
        def function():

            op1 = SQLExecuteQueryOperator(
                task_id=f"{database}.create_schema",
                conn_id=f"trino_{database}",
                sql=f"create schema if not exists {database}.example;"
                )


            op2 = SQLExecuteQueryOperator(
                task_id=f"{database}.create_table",
                conn_id=f"trino_{database}",
                sql=f"create table {database}.example.test as (SELECT 'Hello, World!' AS greeting);"
                )
        
            op1 >> op2

        function() >> final_op

    
dag