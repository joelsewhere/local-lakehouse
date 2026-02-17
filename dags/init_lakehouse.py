from airflow.sdk import dag, task

CATALOG = [
    'raw',
    'api',
    'main',
    ]

def polaris_headers():

    from requests import post

    url = 'http://polaris:8181/api/catalog/v1/oauth/tokens'
    data = {
        'grant_type': 'client_credentials',
        'client_id': 'root',
        'client_secret': 'secret',
        'scope': 'PRINCIPAL_ROLE:ALL'
    }

    response = post(url, data=data)

    access_token = response.json()['access_token']

    headers = {
        'Authorization': f'Bearer {access_token}'
        }
    
    return headers



@dag
def init_lakehouse():

    @task
    def create_engineering_role():
        from requests import post

        headers = polaris_headers()

        # Data engineer role
        url = 'http://polaris:8181/api/management/v1/principal-roles'
        json = {"principalRole":{"name":"data_engineer"}}
        print(post(url, json=json, headers=headers).json())


    final_catalog_nodes = []


        
    @task(map_index_template='{{ catalog }}')
    def create_catalog(catalog):
        from requests import post

        headers = polaris_headers()

        json = {
            'name': catalog,
            'type': 'INTERNAL',
            'properties': {
                "default-base-location": f"s3://warehouse/{catalog}",
                "s3.endpoint": "http://minio:9000",
                "s3.path-style-access": "true",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "dummy-region"
                },
            "storageConfigInfo": {
            "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
            "storageType": "S3",
            "allowedLocations": [
                f"s3://warehouse/{catalog}/*"
            ]
            }
        }

        url = 'http://polaris:8181/api/management/v1/catalogs'

        print(post(url, headers=headers, json=json).json())

        return catalog

    @task(map_index_template='{{ catalog }}_admin_role')
    def admin_role(catalog):
        from requests import put

        headers = polaris_headers()
        # Catalog admin role
        url = f'http://polaris:8181/api/management/v1/catalogs/{catalog}/catalog-roles/catalog_admin/grants'
        json = {"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}
        print(put(url, json=json, headers=headers).text)

        return catalog

    @task(map_index_template="{{catalog}}__admin_engineering_role")
    def admin_engineering(catalog):

        from requests import put
    
        headers = polaris_headers()
        # Connect the roles
        url = f'http://polaris:8181/api/management/v1/principal-roles/data_engineer/catalog-roles/{catalog}'
        json = {"catalogRole":{"name":"catalog_admin"}}
        print(put(url, json=json, headers=headers).text)

        return catalog

    catalogs = create_catalog.partial().expand(catalog=CATALOG)
    admin_roles = admin_role.partial().expand(catalog=catalogs)
    engineering_permissions = admin_engineering.partial().expand(catalog=admin_roles)

    # final = admin_engineering(catalog)
    # create_catalog(catalog) >> engineering >> admin_role(catalog) >> final

    # final_catalog_nodes.append(final)

    
    @task
    def set_principal_role():
        from requests import put

        headers = polaris_headers()
        url = 'http://polaris:8181/api/management/v1/principals/root/principal-roles'
        json = {"principalRole": {"name":"data_engineer"}}
        print(put(url, json=json, headers=headers).text)

    @task
    def confirm_catalogs():
        from requests import get

        headers = polaris_headers()

        print(get('http://polaris:8181/api/management/v1/catalogs', headers=headers).json())


    @task
    def confirm_roles():
        from requests import get

        headers = polaris_headers()

        print(get("http://polaris:8181/api/management/v1/principals/root/principal-roles", headers=headers))


    create_engineering_role() >> catalogs >> set_principal_role() >> confirm_catalogs() >> confirm_roles()


init_lakehouse()


        