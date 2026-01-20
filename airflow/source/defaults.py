import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


timezone = pendulum.timezone('America/Chicago')

def datetime(*args, **kwargs):

    tz_kwargs = {'tz': timezone}
    tz_kwargs.update(kwargs)

    return pendulum.datetime(*args, **tz_kwargs)
    
