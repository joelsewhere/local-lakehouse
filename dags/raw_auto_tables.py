from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from datetime import datetime
import pyarrow as pa

BUCKET = 'warehouse'

"""
TODO
- Add filetype handle logic
    - .json, .csv, .parquet
- Seperate schema santization from filehandling. Santization should happen after
  the file has been read in as a pyarrow table
"""

def load_catalog():

    from pyiceberg.catalog.rest import HttpMethod
    import pyiceberg.catalog.rest as rest_module

    # Patch HttpMethod to accept any HTTP method Polaris returns
    original_missing = getattr(HttpMethod, "_missing_", None)

    def _missing_(cls, value):
        obj = str.__new__(cls, value)
        obj._name_ = value
        obj._value_ = value
        return obj

    HttpMethod._missing_ = classmethod(_missing_)

    from pyiceberg.catalog import load_catalog as load_catalog_

    POLARIS_URI = "http://polaris:8181/api/catalog/" # Example URI

    catalog = load_catalog_(
        "raw", # A name for your catalog instance
        **{
            "type": "rest",
            "uri": POLARIS_URI,
            "credential": "root:secret",
            "warehouse": "raw",
            "scope": "PRINCIPAL_ROLE:data_engineer",
        }
        )
    
    return catalog

def parse_stringified_json(value):
    import json
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            # Only replace if the result is a dict or list, not a plain scalar
            if isinstance(parsed, (dict, list)):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    return value
    
def align_batch_to_schema(batch, table_schema):
    """
    Add any missing columns (as nulls) to the batch so it matches the
    full Iceberg table schema — required before appending.
    """
    import pyarrow as pa
    iceberg_field_names = [f.name for f in table_schema.fields]
    
    for field_name in iceberg_field_names:
        if field_name not in batch.schema.names:
            batch = batch.append_column(
                field_name,
                pa.array([None] * len(batch), type=pa.null())
            )
    
    # Reorder columns to match iceberg schema
    batch = batch.select([f for f in iceberg_field_names if f in batch.schema.names])
    return batch

def load_file(path):
    import pyarrow as pa
    import json

    hook = S3Hook()
    json_string = hook.read_key(key=path, bucket_name=BUCKET)
    json_list = json.loads(json_string)

    for record in json_list:
        for key, value in record.items():
            record[key] = parse_stringified_json(value)
    
    table = pa.Table.from_pylist(json_list)

    return table

def find_new_columns(pytable, iceberg):

    existing_names = {f.name for f in iceberg.schema().fields}
    new_fields = [f for f in pytable.schema if f.name not in existing_names]

    return new_fields


def update_iceberg_schema(new_columns, iceberg):
    import pyarrow
    from pyiceberg.io.pyarrow import pyarrow_to_schema


    new_partial_schema = pyarrow_to_schema(pyarrow.schema(new_columns))
    with iceberg.update_schema() as update:
        for field in new_partial_schema.fields:
            update.add_column(path=field.name, field_type=field.field_type, required=False)

    namespace = '.'.join(iceberg.identifier[:-1])
    table = iceberg.identifier[-1]

    catalog = load_catalog()
    iceberg_table = catalog.load_table((namespace, table))

    batch = align_batch_to_schema(batch, iceberg_table.schema())

    return iceberg_table

def sanitize_arrow_schema(schema):
    import pyarrow as pa
    new_fields = []
    for field in schema:
        new_fields.append(sanitize_arrow_field(field))
    # Remove metadata at schema level AND rebuild each field without metadata
    return pa.schema(new_fields, metadata={})  # no metadata carried over since fields are rebuilt

def sanitize_arrow_field(field):
    import pyarrow as pa

    if pa.types.is_null(field.type):
        return pa.field(field.name, pa.string(), nullable=True)
    elif pa.types.is_timestamp(field.type):
        # Downcast ns to us - Iceberg does not support ns precision
        tz = field.type.tz
        return pa.field(field.name, pa.timestamp("us", tz=tz), nullable=True)
    elif pa.types.is_list(field.type):
        value_type = field.type.value_type
        if pa.types.is_null(value_type):
            return pa.field(field.name, pa.list_(pa.string()), nullable=True)
        return pa.field(field.name, field.type, nullable=True)
    elif pa.types.is_struct(field.type):
        sanitized = [sanitize_arrow_field(f) for f in field.type]
        return pa.field(field.name, pa.struct(sanitized), nullable=True)
    else:
        return pa.field(field.name, field.type, nullable=True)
    
def replace_null_types(schema: pa.Schema, fill_type=pa.string()):
    """Recursively replace pa.null() with a concrete type in a schema."""
    new_fields = []
    for field in schema:
        new_fields.append(replace_null_field(field, fill_type))
    return pa.schema(new_fields)

def replace_null_field(field, fill_type):
    import pyarrow as pa 

    if pa.types.is_null(field.type):
        return field.with_type(fill_type)
    elif pa.types.is_struct(field.type):
        new_struct_fields = [replace_null_field(f, fill_type) for f in field.type]
        return field.with_type(pa.struct(new_struct_fields))
    elif pa.types.is_list(field.type):
        new_value_field = replace_null_field(field.type.value_field, fill_type)
        return field.with_type(pa.list_(new_value_field))
    elif pa.types.is_large_list(field.type):
        new_value_field = replace_null_field(field.type.value_field, fill_type)
        return field.with_type(pa.large_list(new_value_field))
    elif pa.types.is_map(field.type):
        new_item_field = replace_null_field(field.type.item_field, fill_type)
        return field.with_type(pa.map_(field.type.key_type, new_item_field.type))
    else:
        return field
    
def sanitize_arrow_table(table):
    clean_schema = sanitize_arrow_schema(table.schema)
    return table.cast(clean_schema)

def create_iceberg_table(file, namespace, table_name):
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids

    catalog = load_catalog()
    arrow_table = load_file(file)

    if not catalog.namespace_exists(namespace):
        catalog.create_namespace(namespace)

    # Sanitize the table first, then derive schema from the sanitized version
    clean_table = sanitize_arrow_table(arrow_table)
    new_schema = replace_null_types(clean_table.schema, fill_type=pa.string())
    clean_table = clean_table.cast(new_schema)
    iceberg_schema = _pyarrow_to_schema_without_ids(clean_table.schema)

    catalog.create_table(
        (namespace, table_name),
        schema=iceberg_schema,
    )


@dag(params={
        "prefix": Param('landing/unprocessed/')
        }
    )
def raw_ingester():

    @task_group
    def build_unprocessed_file_workers():

        @task
        def get_unprocessed_namespaces(params):

            paths = S3Hook().list_prefixes(
                bucket_name=BUCKET,
                prefix=params['prefix'],
                delimiter='/'
                )
            
            return paths
        
        @task(map_index_template = '{{ namespace }}')
        def get_unprocessed_tables(namespace):

            paths = S3Hook().list_prefixes(
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

            paths = S3Hook().list_keys(
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

        @task_group
        def create_table(files):

            @task.branch(map_index_template = '{{ namespace }}.{{ table }}')
            def check_if_table_exists(files):

                print(files)
                
                context = get_current_context()
                namespace = files[0].split('/')[2]
                table = files[0].split("/")[3]
                context['namespace'] = namespace
                context['table'] = table

                catalog = load_catalog()

                if catalog.table_exists(f'{namespace}.{table}'):
                    return  'process_files.update_schema'
                else:
                    return 'process_files.create_table.create_new_table'

            @task(map_index_template = '{{ namespace }}.{{ table }}')
            def create_new_table(files):
                
                context = get_current_context()
                namespace = files[0].split('/')[2]
                table = files[0].split("/")[3]
                context['namespace'] = namespace
                context['table'] = table

                create_iceberg_table(files[0], namespace, table)

            check_table = check_if_table_exists(files) 
            check_table >> create_new_table(files)


        @task_group
        def update_schema(files):

            @task.branch(map_index_template = '{{ namespace }}.{{ table }}')
            def check_for_schema_change(files):

                context = get_current_context()
                namespace = files[0].split('/')[2]
                table = files[0].split("/")[3]
                context['namespace'] = namespace
                context['table'] = table

                catalog = load_catalog()
                iceberg = catalog.load_table((namespace, table))

                new_schema_files = []
                for file in files:
                    pytable = load_file(file)
                    clean_table = sanitize_arrow_table(pytable)
                    new_schema = replace_null_types(clean_table.schema, fill_type=pa.string())
                    clean_table = clean_table.cast(new_schema)
                    if find_new_columns(clean_table, iceberg):
                        new_schema_files.append(file)

                if new_schema_files:
                    context['ti'].xcom_push(key="new_schema_files", value=new_schema_files)
                    return 'update_schema.set_new_schema'

                return ['process_files.process']

            @task(map_index_template = '{{ namespace }}.{{ table }}')
            def set_new_schema(files):
                
                context = get_current_context()
                namespace = files[0].split('/')[2]
                table = files[0].split("/")[3]
                context['namespace'] = namespace
                context['table'] = table

                catalog = load_catalog()
                iceberg = catalog.load_table((namespace, table))

                new_schema_files = context['ti'].xcom_pull(task_ids='update_schema.check_for_schema_change', key='new_schema_files')
                for file in new_schema_files:

                    pytable = load_file(file)
                    clean_table = sanitize_arrow_table(pytable)
                    new_schema = replace_null_types(clean_table.schema, fill_type=pa.string())
                    clean_table = clean_table.cast(new_schema)
                    new_columns = find_new_columns(clean_table, iceberg)
                    iceberg = update_iceberg_schema(new_columns, iceberg)

            check_schema = check_for_schema_change(files)
            check_schema >> set_new_schema(files)

            return check_schema

        @task(trigger_rule='all_done', map_index_template = '{{ namespace }}.{{ table }}')
        def process(files):
            context = get_current_context()
            namespace = files[0].split('/')[2]
            table = files[0].split("/")[3]
            context['namespace'] = namespace
            context['table'] = table

            catalog = load_catalog()
            iceberg = catalog.load_table((namespace, table))

            for file in files:

                pytable = load_file(file)
                clean_table = sanitize_arrow_table(pytable)
                clean_table = sanitize_arrow_table(pytable)
                new_schema = replace_null_types(clean_table.schema, fill_type=pa.string())
                clean_table = clean_table.cast(new_schema)
                clean_table = align_batch_to_schema(clean_table, iceberg.schema())
                iceberg.append(clean_table)


        table_exists = create_table(files=files)
        schema_changes = update_schema(files=files) 
        load = process(files)

        table_exists >> schema_changes >> load
 
    files = build_unprocessed_file_workers()
    process = process_files.partial().expand(files=files)

    @task(trigger_rule='none_failed')
    def processed_files_to_cold_storage(files):

        hook = S3Hook()

        for file in files:
            
            file_path_parts = file.split('/')
            file_path_parts[1] = 'processed'
            dest_key = '/'.join(file_path_parts)

            hook.copy_object(
                source_bucket_name=BUCKET,
                source_bucket_key=file,
                dest_bucket_name=BUCKET,
                dest_bucket_key=dest_key
            )

            hook.delete_objects(
                bucket=BUCKET,
                keys=file
            )

    process >> processed_files_to_cold_storage.partial().expand(files=files)

            


raw_ingester()
    