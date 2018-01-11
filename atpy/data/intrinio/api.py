import itertools
import os
from io import StringIO

import pandas as pd
import requests.sessions as sessions


def to_dataframe(csv_str: str):
    """
    Convert csv result to DataFrame
    :param csv_str: csv string
    :return: pd.DataFrame
    """
    dates = [d for d in csv_str.split('\n', 1)[0].split(',') if 'date' in d or 'period' in d]
    return pd.read_csv(StringIO(csv_str), parse_dates=dates)


def get_csv(sess: sessions.Session, endpoint: str, **parameters):
    """
    get csv data from the Intrinio API
    :param sess: session
    :param endpoint: endpoint
    :param parameters: query parameters
    :return: csv result
    """
    auth = os.getenv('INTRINIO_USERNAME'), os.getenv('INTRINIO_PASSWORD')

    url = '{}/{}'.format('https://api.intrinio.com', endpoint)

    if 'page_size' not in parameters:
        parameters['page_size'] = 10000

    pages = list()

    for page_number in itertools.count():
        parameters['page_number'] = page_number + 1

        response = sess.request('GET', url, params=parameters, auth=auth, verify=True)
        if not response.ok:
            response.raise_for_status()

        info, columns, page = response.content.decode('utf-8').split('\n', 2)

        if page_number == 0:
            info = {s.split(':')[0]: s.split(':')[1] for s in info.split(',')}
            total_pages = int(info['TOTAL_PAGES'])
            pages.append(columns.lower() + '\n')

        pages.append(page)

        if len(page) == 0 or page_number + 1 == total_pages:
            break

    return ''.join(pages)


class IntrinioEvents(object):
    """
    Intrinio requests via events
    """

    def __init__(self, listeners):
        self.listeners = listeners
        self.listeners += self.listener

    def listener(self, event):
        if event['type'] == 'intrinio_request':
            with sessions.Session() as sess:
                endpoint = event['endpoint'] if event['endpoint'].endswith('.csv') else event['endpoint'] + '.csv'
                if 'parameters' in event:
                    result = get_csv(sess, endpoint=endpoint, **event['parameters'])
                else:
                    result = get_csv(sess, endpoint=endpoint)

            if 'dataframe' in event:
                result = to_dataframe(result)

            self.listeners({'type': 'intrinio_request_result', 'data': result})
