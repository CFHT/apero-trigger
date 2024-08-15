import json
from typing import Dict, Mapping, Union, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

from astropy.io import fits

from logger import log

FitsHeaderValue = Union[str, int, float, complex, bool]
FitsHeaderCard = Union[FitsHeaderValue, Tuple[FitsHeaderValue, str]]
FitsHeaderDict = Dict[str, FitsHeaderCard]
JsonObj = Mapping[str, any]

KEY_FILE = '/h/spirou/bin/.cfht_access'


class QsoDatabase:
    def __init__(self):
        try:
            with open(KEY_FILE, 'r') as file_read:
                self.bearer_token = file_read.read().strip()
        except OSError:
            log.error('Failed to load API bearer token, will not be able to access database', exc_info=False)
            self.bearer_token = None

    def send_pipeline_headers(self, header_dict: FitsHeaderDict, in_progress=False):
        if not self.bearer_token:
            log.warning('No bearer token loaded, cannot send values to the database')
            return
        data = {
            'bearer_token': self.bearer_token,
            **header_dict
        }
        url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
        if in_progress:
            url = url + "-in-progress"
        try:
            self.json_request(url, data, retries=2)
        except URLError:
            log.error('Error sending values %s to database', header_dict, exc_info=True)

    @staticmethod
    def json_request(url: str, data: JsonObj, headers: Mapping[str, str] = None, retries=0) -> JsonObj:
        http_headers = {'Content-Type': 'application/json'}
        if headers:
            http_headers.update(headers)
        json_data = json.dumps(data)
        request = Request(url, json_data.encode('utf-8'), http_headers)
        for attempt in range(retries + 1):
            try:
                response = urlopen(request)
            except URLError:
                if attempt == retries:
                    raise
            else:
                return json.loads(response.read().decode('utf-8'))


class DatabaseHeaderConverter:
    @staticmethod
    def preprocessed_header_to_db(header: fits.Header) -> JsonObj:
        return {
            'dprtype': header['DPRTYPE']
        }

    @staticmethod
    def extracted_header_to_db(header: fits.Header) -> JsonObj:
        return {
            'dprtype': header['DPRTYPE'],
            'snr10': header['EXTSN010'],
            'snr34': header['EXTSN034'],
            'snr44': header['EXTSN044']
        }
