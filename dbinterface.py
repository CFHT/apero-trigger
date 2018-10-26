import json
from urllib.request import Request, urlopen

from logger import logger

keyfile = '/h/spirou/bin/.cfht_access'

try:
    with open(keyfile, 'r') as fileread:
        bearer_token = fileread.read().strip()
except:
    logger.error('Failed to load API bearer token', exc_info=True)
    bearer_token = None

def send_headers_to_db(header_dict):
    if not bearer_token:
        logger.warning('No bearer token loaded, cannot send values to the database')
    data = json.dumps({
        'bearer_token': bearer_token,
        **header_dict
    })
    http_headers = {'Content-Type': 'application/json'}
    url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
    try:
        request = Request(url, data.encode('utf-8'), http_headers)
        urlopen(request)
    except:
        logger.error('Error sending values %s to database', header_dict, exc_info=True)
