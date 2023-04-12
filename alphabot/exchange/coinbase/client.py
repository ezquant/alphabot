import json
import hmac
import hashlib
import time
import base64
import requests
from urllib.parse import urlencode, quote_plus

from alphabot import logger


class CoinbaseClient():
    def __init__(self, api_key, secret_key, passphrase, url):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.url = url

    def submit_order(self, order):
        path = '/orders'
        return self._make_request('POST', path, data=order)

    def get_accounts(self):
        path = '/accounts'
        return self._make_request('GET', path)

    def get_order(self, order_id):
        path = '/orders/%s' % order_id
        return self._make_request('GET', path)

    def get_account(self, account_id):
        path = '/accounts' + '/' + account_id
        return self._make_request('GET', path)

    def get_fills(self, order_id):
        path = '/fills?order_id={order_id}'.format(order_id=order_id)
        return self._make_request('GET', path)

    def get_historic_rates(self, product_id: str, start: str, end: str, granularity: int):
        path_dict = {
            'start': start,
            'end': end,
            'granularity': granularity
        }
        params = urlencode(path_dict, quote_via=quote_plus)
        path = '/products/{product_id}/candles?'.format(product_id=product_id)
        path = path + params
        return self._make_request('GET', path)

    def _make_request(self, method, path, data=None):
        timestamp = str(time.time())
        message = timestamp + method + path
        if data:
            message = message + json.dumps(data)

        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode().rstrip('\n')

        headers = {
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        url = self.url + path
        try:
            response = requests.request(method, url, json=data, headers=headers)
            logger.debug("Response: " + str(response.content))
        except Exception as e:
            logger.error('Error during connection %s', e)
            return

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error('HTTP error %s', e)
            print(response.text)
            return

        return response
