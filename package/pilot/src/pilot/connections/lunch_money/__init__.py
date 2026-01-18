from pilot.connections.decorators import get, url_factory
from pilot.utils import Environment
from typing import Optional

ENV = Environment()
ROOT = 'https://dev.lunchmoney.app'
HEADERS = {
    'Authorization': f'Bearer {ENV["LUNCHMONEY_API_KEY"]}',
    }
url = url_factory(ROOT, headers=HEADERS)


@get
@url
def get_transactions(self, **kwargs) -> dict:
    """
    https://lunchmoney.dev/#get-all-transactions
    """

    return '/v1/transactions'


    


