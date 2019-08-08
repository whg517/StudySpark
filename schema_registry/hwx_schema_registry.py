import json
from urllib.parse import urljoin

from requests_kerberos import HTTPKerberosAuth

from schema_registry.schema import SchemaRegistry


class HWXSchemaRegistry(SchemaRegistry):

    def __init__(self, url='http://m2.node.hadoop:7788/api/v1/schemaregistry/'):
        """"""
        super().__init__(url)

    def _get_url(self, path: str) -> str:
        """"""
        return urljoin(self.url, path)

    def _auth_request(self, url) -> dict:
        return self.request(url, auth=HTTPKerberosAuth())

    def latest(self, name):
        """"""
        url_path = f'schemas/{name}/versions/latest'
        schema_text = self._auth_request(self._get_url(url_path)).get('schemaText')
        return json.loads(schema_text)
