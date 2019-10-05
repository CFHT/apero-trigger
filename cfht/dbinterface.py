import json
from collections import defaultdict, OrderedDict
from urllib.error import URLError
from urllib.request import Request, urlopen

from trigger import log

keyfile = '/h/spirou/bin/.cfht_access'


class QsoDatabase:
    def __init__(self):
        try:
            with open(keyfile, 'r') as fileread:
                self.bearer_token = fileread.read().strip()
        except:
            log.error('Failed to load API bearer token, will not be able to access database', exc_info=False)
            self.bearer_token = None

    def send_pipeline_headers(self, header_dict):
        if not self.bearer_token:
            log.warning('No bearer token loaded, cannot send values to the database')
            return
        data = {
            'bearer_token': self.bearer_token,
            **header_dict
        }
        url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
        try:
            self.json_request(url, data)
        except:
            log.error('Error sending values %s to database', header_dict, exc_info=True)

    def get_exposure(self, obsid):
        result = self.get_exposure_range(obsid, obsid)
        if result:
            return result[0]

    def get_exposure_range(self, first, last):
        try:
            return self.get_exposures_status({
                'obsid_range': {
                    'first': first,
                    'last': last
                }
            })
        except URLError:
            log.error('Error fetching exposures for obsid range %s-%s', first, last, exc_info=True)

    def get_exposures_status(self, request_data):
        if not self.bearer_token:
            log.warning('No bearer token loaded, cannot fetch values from the database')
            return None
        auth_headers = {'Authorization': 'Bearer ' + self.bearer_token}
        url = 'https://api.cfht.hawaii.edu/op/exposures'
        response_data = self.json_request(url, request_data, auth_headers)
        return [exposure['exposure_status'] for exposure in response_data['exposure']]

    @staticmethod
    def json_request(url, data, headers=None):
        http_headers = {'Content-Type': 'application/json'}
        if headers:
            http_headers.update(headers)
        json_data = json.dumps(data)
        request = Request(url, json_data.encode('utf-8'), http_headers)
        response = urlopen(request)
        return json.loads(response.read().decode('utf-8'))


class DatabaseHeaderConverter:
    @staticmethod
    def extracted_header_to_db(header):
        return {
            'dprtype': header['DPRTYPE'],
            'snr10': header['SNR10'],
            'snr34': header['SNR34'],
            'snr44': header['SNR44']
        }

    @staticmethod
    def ccf_header_to_db(header):
        return {
            'ccfmask': header['CCFMASK'],
            'ccfmacpp': header['CCFMACP'],
            'ccfcontr': header['CCFCONT'],
            'ccfrv': header['CCFRV'],
            'ccfrvc': header['CCFRVC'],
            'ccffwhm': header['CCFFWHM']
        }

    @staticmethod
    def exp_status_db_to_header(exposure_status):
        return {
            'QSOVALID': (exposure_status['exp_status'], 'QSO validation state'),
            'QSOGRADE': (exposure_status.get('grade'), 'QSO grade (1=good 5=unusable)'),
        }

    @classmethod
    def seq_status_db_to_header(cls, exposure_statuses):
        per_key = defaultdict(OrderedDict)
        for i, exposure_status in enumerate(exposure_statuses):
            header_vals = cls.exp_status_db_to_header(exposure_status)
            for key, value in header_vals.items():
                new_key = cls.indexed_header_key(key, i + 1)
                per_key[key][new_key] = value
        combined = {key: value for current in per_key.values() for key, value in current.items()}
        return combined

    @staticmethod
    def indexed_header_key(key, index):
        # Note: this can still produce keywords over 8 chars if there are more than 9 values
        if len(key) < 8:
            return key + str(index)
        else:
            return key[:7] + str(index)
