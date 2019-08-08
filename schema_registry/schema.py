import json
from urllib.parse import urljoin
import requests
from requests_kerberos import HTTPKerberosAuth


class SchemaRegistry(object):

    def __init__(self, url):
        self.url = url

    def request(self, url: str, *args, **kwargs) -> dict:
        """

        :param url:
        :param args:
        :param kwargs:
        :return:
        """
        response = requests.get(url, *args, **kwargs)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f'Response code is not 200. <{response.status_code}>')
