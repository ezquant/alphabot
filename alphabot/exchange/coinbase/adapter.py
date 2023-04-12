from alphabot.exchange.coinbase.client import CoinbaseClient
from alphabot.config import config
from alphabot.exchange import BaseAdapter


class CoinbaseAdapter(BaseAdapter):

    def __init__(self):
        self._get_credentials()
        self.client = CoinbaseClient(self.access_key, self.secret_key, self.passphrase, self.url)

    def _get_credentials(self):
        # Set credentials.
        self.access_key = config['coinbase']['access_key']
        self.secret_key = config['coinbase']['secret_key']
        self.passphrase = config['coinbase']['passphrase']
        self.url = config['coinbase']['url']

    def get_accounts(self):
        response = self.client.get_accounts()
        return response.json()

    def get_account(self, currency):
        """
        Example account response;
        {
            'id': 'e6a8d351-027e-487f-94e7-985a9a075090',
            'currency': 'USD',
            'balance': '1735.7693716566837000',
            'available': '1734.7663716566837',
            'hold': '1.0030000000000000',
            'profile_id': 'a6c82597-f923-4e68-a255-21ce5791c8ea'
         }
        """
        account_id = config['coinbase']['accounts'][currency]
        response = self.client.get_account(account_id)
        return response.json()

    def get_order(self, order_id):
        """
        Example order response;
        {
            'id': 'ce110d8d-14e4-406a-98e9-6d089158e902',
            'size': '0.95090000',
            'product_id': 'ETH-USD',
            'side': 'sell',
            'type': 'market',
            'post_only': False,
            'created_at': '2018-12-13T11:27:50.061553Z',
            'done_at': '2018-12-13T11:27:50.093Z',
            'done_reason': 'filled',
            'fill_fees': '0.1711620000000000',
            'filled_size': '0.95090000',
            'executed_value': '57.0540000000000000',
            'status': 'done',
            'settled': True}
        """
        response = self.client.get_order(order_id)
        return response.json()

    def get_fills(self, order_id):
        response = self.client.get_fills(order_id)
        if not response:
            return

        return response.json()

    def get_historic_rates(self, product_id: str, start: str, end: str, granularity: int):
        response = self.client.get_historic_rates(product_id, start, end, granularity)
        if not response:
            return

        return response.json()

    def submit_order(self, order):
        """
        Example order response;
        {'created_at': '2018-12-11T12:12:30.607493Z',
        'executed_value': '0.0000000000000000',
        'fill_fees': '0.0000000000000000',
        'filled_size': '0.00000000',
        'funds': '1729.5776387400000000',
        'id': 'eea70b05-7bff-4f5c-9f90-49dbfe497555',
        'post_only': False,
        'product_id': 'ETH-USD',
        'settled': False,
        'side': 'buy',
        'size': '1735.76937165',
        'status': 'pending',
        'stp': 'dc',
        'type': 'market'}
        """
        response = self.client.submit_order(order)
        if not response:
            return

        return response.json()
