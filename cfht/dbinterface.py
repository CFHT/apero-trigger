import json
from collections import defaultdict, OrderedDict
from typing import Dict, Mapping, List, Union, Tuple, Collection
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

    def get_exposure(self, obsid: int) -> JsonObj:
        result = self.get_exposure_range(obsid, obsid)
        if result:
            return result[0]

    def get_exposure_range(self, first: int, last: int) -> List[JsonObj]:
        try:
            return self.get_exposures_status({
                'obsid_range': {
                    'first': first,
                    'last': last
                }
            })
        except URLError:
            log.error('Error fetching exposures for obsid range %s-%s', first, last, exc_info=True)

    def get_exposures_status(self, request_data: JsonObj) -> List[JsonObj]:
        if not self.bearer_token:
            log.warning('No bearer token loaded, cannot fetch values from the database')
            return []
        auth_headers = {'Authorization': 'Bearer ' + self.bearer_token}
        url = 'https://api.cfht.hawaii.edu/op/exposures'
        response_data = self.json_request(url, request_data, headers=auth_headers, retries=2)
        return [exposure['exposure_status'] for exposure in response_data['exposure']]

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

    @staticmethod
    def ccf_header_to_db(header: fits.Header) -> JsonObj:
        return {
            'ccfmask': header['CCFMASK'],
            'ccfmacpp': 0,
            'ccfcontr': header['CCFMCONT'],
            'ccfrv': header['RV_OBJ'],
            'ccfrvc': header['RV_CORR'],
            'ccffwhm': header['CCFMFWHM']
        }

    @staticmethod
    def exp_status_db_to_header(exposure_status: JsonObj) -> FitsHeaderDict:
        return {
            'QSOVALID': (exposure_status['exp_status'], 'QSO validation state'),
            'QSOGRADE': (exposure_status.get('grade'), 'QSO grade (1=good 5=unusable)'),
        }

    @classmethod
    def seq_status_db_to_header(cls, exposure_statuses: Collection[JsonObj]) -> FitsHeaderDict:
        per_key = defaultdict(OrderedDict)
        for i, exposure_status in enumerate(exposure_statuses):
            header_cards = cls.exp_status_db_to_header(exposure_status)
            for key, value in header_cards.items():
                new_key = cls.indexed_header_key(key, i + 1, len(exposure_statuses))
                per_key[key][new_key] = value
        combined = {key: value for current in per_key.values() for key, value in current.items()}
        return combined

    @staticmethod
    def indexed_header_key(key: str, index: int, max_index: int) -> str:
        n_digits = len(str(max_index))
        n_key_chars = min(8 - n_digits, len(key))
        return key[:n_key_chars] + str(index)
