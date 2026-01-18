import os
import shutil
from pathlib import Path
import dotenv 



class Environment:

    CONFIG_DIR = Path.home() / '.pilot'
    ENV = CONFIG_DIR / '.env.variables'

    def __getitem__(self, key):
        failures = []

        try:
            return os.environ[key]
        except Exception as failure:
            failures.append(failure)

        try:
            from airflow.models import Variable
            return Variable.get(key)
        except Exception as failure:
            failures.append(failure)


        
        raise KeyError(
            'The following error were thrown '
            f'{[str(x) for x in failures]}')
    

STARTER_ENV = Path(__file__).parent / '.env.variables'

if not Environment.CONFIG_DIR.is_dir():

    Environment.CONFIG_DIR.mkdir()

    shutil.copy(STARTER_ENV, Environment.ENV)
    
else:

    starter_env = dotenv.dotenv_values(STARTER_ENV)
    existing_env = dotenv.dotenv_values(Environment.ENV)

    update_env = {
        key:value for key, value in starter_env.items() 
        if key not in existing_env.keys()
        }
    
    with Environment.ENV.open('a') as file:
        
        for key, value in update_env.items():

            file.write(f'{key}="{value}"')

dotenv.load_dotenv(Environment.ENV)


    



