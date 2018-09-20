import json
from urllib.request import Request, urlopen

keyfile = '/h/spirou/bin/.cfht_access'

with open(keyfile, 'r') as fileread:
    bearer_token = fileread.read().strip()

def send_headers_to_db(obsid, dprtype, snr10, snr34, snr44):
    data = json.dumps({
        'bearer_token': bearer_token,
        'obsid': obsid,
        'snr10': snr10,
        'snr34': snr34,
        'snr44': snr44,
        'dprtype': dprtype
    })
    headers = {'Content-Type': 'application/json'}
    url = 'https://op-api.cfht.hawaii.edu/op-cli/op-spirou-update-pipeline'
    request = Request(url, data.encode('utf-8'), headers)
    urlopen(request)
