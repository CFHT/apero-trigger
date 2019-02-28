import json
from urllib.error import URLError
from urllib.request import Request, urlopen

from logger import logger

keyfile = '/h/spirou/bin/.cfht_access'


class QsoDatabase:
    def __init__(self):
        try:
            with open(keyfile, 'r') as fileread:
                self.bearer_token = fileread.read().strip()
        except:
            logger.error('Failed to load API bearer token, will not be able to access database', exc_info=False)
            self.bearer_token = None

    def send_pipeline_headers(self, header_dict):
        if not self.bearer_token:
            logger.warning('No bearer token loaded, cannot send values to the database')
            return
        data = {
            'bearer_token': self.bearer_token,
            **header_dict
        }
        url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
        try:
            self.json_request(url, data)
        except:
            logger.error('Error sending values %s to database', header_dict, exc_info=True)

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
            logger.error('Error fetching exposures for obsid range %s-%s', first, last, exc_info=True)

    def get_exposures_status(self, request_data):
        if not self.bearer_token:
            logger.warning('No bearer token loaded, cannot fetch values from the database')
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
