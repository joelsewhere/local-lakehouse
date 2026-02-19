import functools
import requests

def url_factory(root, headers={}):

    def decorator(func):

        @functools.wraps(func)
        def construct_url(*args, **kwargs):
            
            endpoint = func(*args, **kwargs)
            if isinstance(endpoint, dict):
                endpoint['url'] = root + endpoint['url']
                endpoint.get('headers', {}).update(headers)
            elif isinstance(endpoint, str):
                endpoint = root + endpoint
                if headers:
                    endpoint = {'url': endpoint, 'headers': headers}

            return endpoint
            
        return construct_url
    
    return decorator


def request_factory(method):

    def decorator(func):

        @functools.wraps(func)  # Preserves func's metadata
        def send_request(*args, **kwargs):
            
            request_data = func(*args, **kwargs)
            if isinstance(request_data, dict):
                return requests.request(
                    method=method,
                    **request_data
                    )
            elif isinstance(request_data, str):
                return requests.request(
                    method=method,
                    url=request_data
                    )
            
            return func(*args, **kwargs)
        
        return send_request
    
    return decorator

get = request_factory('GET')
