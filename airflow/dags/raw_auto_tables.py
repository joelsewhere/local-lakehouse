from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from source import defaults
from airflow.operators.python import get_current_context

BUCKET = 'warehouse'


@dag(
    schedule=None,
    start_date=defaults.datetime(2026, 1, 1),
    catchup=False,
    params={
        "prefix": Param('landing/unprocessed/')
        }
    )
def raw_ingester():

    @task_group
    def build_unprocessed_file_workers():

        @task
        def get_unprocessed_namespaces(params):

            paths = defaults.S3Hook().list_prefixes(
                bucket_name=BUCKET,
                prefix=params['prefix'],
                delimiter='/'
                )
            
            return paths
        
        @task(map_index_template = '{{ namespace }}')
        def get_unprocessed_tables(namespace):

            paths = defaults.S3Hook().list_prefixes(
                bucket_name=BUCKET,
                prefix=namespace,
                delimiter='/'
                )
            
            context = get_current_context()
            context['namespace'] = namespace.split('/')[2]
            
            return paths
        

    
        @task(map_index_template = '{{ namespace }}.{{ table }}')
        def get_unprocessed_files(table):

            print(table)

            paths = defaults.S3Hook().list_keys(
                bucket_name=BUCKET,
                prefix=table[0],
                )
            
            context = get_current_context()
            context['namespace'] = table[0].split('/')[2]
            context['table'] = table[0].split('/')[3]

            return paths
        
        namespaces = get_unprocessed_namespaces()
        tables = get_unprocessed_tables.partial().expand(namespace=namespaces)
        files = get_unprocessed_files.partial().expand(table=tables)

        return files

    @task_group 
    def process_files(files):

        @task
        def create_table(files):
            print(files)

        @task.branch
        def check_for_schema_change(files):
            return ['process_files.update_schema']

        @task
        def update_schema(files):
            pass

        @task
        def no_schema_change_detected():
            pass

        @task(trigger_rule='all_done')
        def process(files):
            pass

        (
            create_table(files=files) 
            >> check_for_schema_change(files=files) 
            >> [update_schema(files), no_schema_change_detected()]
            >> process(files)
            )
        

        
    
    
    files = build_unprocessed_file_workers()
    process_files.partial().expand(files=files)
    
    



    # @task
    # def get_unprocessed_filepaths(params):


    #     print(params['prefix'])

    #     paths = defaults.S3Hook().list_keys(
    #         bucket_name='landing',
    #         prefix=params['prefix'],
    #         )
        
    #     schema_groups = {}
        
    #     for path in paths:

    #         split = path.split('/')
    #         schema = split[1]
    #         table = split[2]

    #         print(schema)
    #         print(table)

    #         schema_groups[schema] = schema_groups.get(schema, {})
    #         schema_groups[schema][table] = schema_groups[schema].get(table, []) + [path]

    #     table_groups = []
    #     for schema, tables in schema_groups.items():

    #         for table, files in tables.items():
    #             table_group = {
    #                 'namespace': schema,
    #                 'table': table,
    #                 'files': files
    #                 }
    #             table_groups.append(table_group)

        
    #     return table_groups
    

    # @task(task_id="namespace", map_index_template="{{ namespace }}")
    # def ingest_namespace(namespace, table, files):

    #     context = get_current_context()
    #     context['namespace'] = namespace

    #     return [{"table": table, 'files': files} for ]


    # @task
    # def log_schema(namespace, table, files):
    #     print(namespace, table, files)

    # @task_group
    # def tables(table):

    #     @task
    #     def log_input(table):

    #         print(table)

    #     @task
    #     def load_data(table):

    #         pass
        
    #     log_input(table)
    #     load_data(table)
        
        # log_schema(namespace, table, files)
            
        # tables.expand_kwargs(topics)

    # schemas.expand_kwargs(get_unprocessed_filepaths())


raw_ingester()
    


# Collect all records in landing/unprocessed
# Runs hourly

# Ship them into raw
    # Auto updating schema
    # Upsert
    # Partition by data interval from landing
    # Partition by process date
