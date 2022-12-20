import requests
from requests import ConnectionError, HTTPError, RequestException
import pandas as pd

import datetime
import json


class APIClient:
    """
    Client to interact with DB.

    Allows POST and DELETE requests only.
    """

    def __init__(self):
        self.BASE_URL = 'http://0.0.0.0:8000/api/orders/'

    def post(self, endpoint, df):
        """POST request handler, for orders only"""

        url = f'{self.BASE_URL}{endpoint}/'

        df = df.reset_index(drop=True)

        # Format timestamps to be readable by Django REST Framework
        df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%dT%H:%M%:%SZ')
        df['dispatch_date'] = pd.to_datetime(df['dispatch_date']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['delivery_date'] = pd.to_datetime(df['delivery_date']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        call_count = 5

        while call_count > 0:
            try:
                res = requests.post(url, data=df.to_json(orient='records'),
                                    headers={'Content-Type': 'application/json'})
                print('RESPONSE', res.__dict__)
                return True
            except ConnectionError as e:
                print(f'Error connecting to API: {e}')
                return False
            except HTTPError as e:
                print(f'Error posting request: {e}')
                return False
            except RequestException as e:
                print(f'Some error occurred...: {e}')
            call_count -= 1
        else:
            print('Something happened. Unable to send.')
            return False

    def get(self, endpoint, filter_by_null=None):

        url = f'{self.BASE_URL}{endpoint}'

        if not filter_by_null:
            res = requests.get(url)
            return res
        else:
            res = requests.get(url, params={'filter_by_null': True})
            data = json.loads(res.content)
            return data

    def update(self, endpoint, df):
        """
        Update null delivery columns in "yesterday's" order data.

        Submits a PATCH request to API, updating relevant columns.
        """
        url = f'{self.BASE_URL}{endpoint}/'

        data = self.get(endpoint, filter_by_null=True)
        ids = []

        for index, item in enumerate(data):
            order_number = item['order_number']
            record = df.loc[df['order_number'] == order_number]

            dispatch_status = record['dispatch_status'].values[0]
            dispatch_date = record['dispatch_date'].values[0]
            dispatch_date = pd.to_datetime(dispatch_date)
            t1 = dispatch_date.strftime('%Y-%m-%dT%H:%M%:%SZ')

            delivery_status = record['delivery_status'].values[0]
            delivery_date = record['delivery_date'].values[0]
            delivery_date = pd.to_datetime(delivery_date)
            t2 = delivery_date.strftime('%Y-%m-%dT%H:%M%:%SZ')

            item_id = str(item['id'])

            ids.append(item['id'])

            payload = {
                'dispatch_status': dispatch_status,
                'dispatch_date': t1,
                'delivery_status': delivery_status,
                'delivery_date': t2
            }

            requests.patch(f'{url}{item_id}/', data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'})

        self.destroy('null_orders', ids)
        return True

    def destroy(self, endpoint, ids):
        print('IDS', ids)

        my_lst_str = ','.join(map(str, ids))
        print(my_lst_str)

        url = f'{self.BASE_URL}{endpoint}/delete/'
        if ids:
            res = requests.delete(url)
            print('Destroy response:', res)

        return True

