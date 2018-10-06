import json
from urllib.request import Request, urlopen

keyfile = '/h/spirou/bin/.cfht_access'

with open(keyfile, 'r') as fileread:
    bearer_token = fileread.read().strip()

def send_headers_to_db(header_dict):
    data = json.dumps({
        'bearer_token': bearer_token,
        **header_dict
    })
    http_headers = {'Content-Type': 'application/json'}
    url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
    request = Request(url, data.encode('utf-8'), http_headers)
    urlopen(request)
