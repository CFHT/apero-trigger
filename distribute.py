#!/usr/bin/env python

"""
Standalone distribution script for APERO products.

This grabs the grade/validation from the Kealahou API and inserts into the
headers of the distributed products for APERO v0.7.

Unlike the full trigger, this script requires only Python (>=3.8) and astropy.
This is because the script makes assumptions about files used rather than
reading inputs from the APERO database.
"""

import argparse
import json
import logging
import sys
from collections import defaultdict, OrderedDict
from pathlib import Path
from typing import Collection, Dict, Mapping, List, Union, Tuple, Sequence, Iterable
from urllib.error import URLError
from urllib.request import Request, urlopen

from astropy.io import fits

log = logging.getLogger()

FitsHeaderValue = Union[str, int, float, complex, bool]
FitsHeaderCard = Union[FitsHeaderValue, Tuple[FitsHeaderValue, str]]
FitsHeaderDict = Dict[str, FitsHeaderCard]
JsonObj = Mapping[str, any]

PRODUCT_ROOT = '/data/spirou4/apero-data/offline/out/'
DISTRIBUTION_ROOT = '/data/distribution/spirou/'
KEY_FILE = '/h/spirou/bin/.cfht_access'


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


class QsoDatabase:
    def __init__(self):
        try:
            with open(KEY_FILE, 'r') as file_read:
                self.bearer_token = file_read.read().strip()
        except OSError:
            log.error('Failed to load API bearer token, will not be able to access database', exc_info=False)
            self.bearer_token = None

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
        response_data = json_request(url, request_data, headers=auth_headers, retries=2)
        return [exposure['exposure_status'] for exposure in response_data['exposure']]


def exp_status_db_to_header(exposure_status: JsonObj) -> FitsHeaderDict:
    return {
        'QSOVALID': (exposure_status['exp_status'], 'QSO validation state'),
        'QSOGRADE': (exposure_status.get('grade'), 'QSO grade (1=good 5=unusable)'),
    }


def seq_status_db_to_header(exposure_statuses: Collection[JsonObj]) -> FitsHeaderDict:
    def indexed_header_key(original_key: str, index: int, max_index: int) -> str:
        n_digits = len(str(max_index))
        n_key_chars = min(8 - n_digits, len(original_key))
        return original_key[:n_key_chars] + str(index)

    per_key = defaultdict(OrderedDict)
    for i, exposure_status in enumerate(exposure_statuses):
        header_cards = exp_status_db_to_header(exposure_status)
        for key, value in header_cards.items():
            new_key = indexed_header_key(key, i + 1, len(exposure_statuses))
            per_key[key][new_key] = value
    combined = {key: value for current in per_key.values() for key, value in current.items()}
    return combined


def get_distribution_path(source: Path, run_id: str, distribution_subdirectory: str) -> Path:
    distribution_dir = Path(DISTRIBUTION_ROOT, run_id.lower(), distribution_subdirectory)
    try:
        distribution_dir.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.error('Failed to create distribution directory %s due to', str(distribution_dir), str(err))
    return Path(distribution_dir, source.name)


def distribute_product(product: Path, header_values: FitsHeaderDict, quicklook: bool):
    subdir = 'quicklook' if quicklook else 'reduced'
    try:
        hdulist = fits.open(product)
        run_id = hdulist[0].header['RUNID']
        hdulist[0].header.update(header_values)
        destination = get_distribution_path(product, run_id, subdir)
        hdulist.writeto(destination, overwrite=True)
    except FileNotFoundError as err:
        log.error('Distribution of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Distribution of %s failed', product, exc_info=True)
    else:
        log.info('Distributing %s', destination)


class Distributor:
    def __init__(self, quicklook: bool = False):
        self.quicklook = quicklook
        self.qso_database = QsoDatabase()

    def distribute_all_nights(self):
        nights = self.__find_nights('*')
        self.distribute_nights(nights)

    def distribute_qrun(self, qrunid: str):
        nights = self.__find_nights(qrunid + '-*')
        self.distribute_nights(nights)

    def distribute_nights(self, nights: Iterable[str]):
        for night in nights:
            self.distribute_night(night)

    def distribute_night(self, night: str):
        log.info('Distributing night %s', night)
        night_dir = Path(PRODUCT_ROOT, night)
        products = list(sorted(file for file in night_dir.glob('*.fits') if file.exists()))
        for product in products:
            self.distribute_product(product)

    def distribute_file(self, night: str, file: str):
        product_file = Path(PRODUCT_ROOT, night, file)
        self.distribute_product(product_file)

    def distribute_product(self, product_file: Path):
        odometer = int(product_file.stem[0:-1])
        letter = product_file.stem[-1]
        qso_database: QsoDatabase
        if letter == 'p':
            exposure_statuses = self.qso_database.get_exposure_range(odometer, odometer + 3)
            header_values = seq_status_db_to_header(exposure_statuses)
            distribute_product(product_file, header_values, self.quicklook)
        else:
            exposure_status = self.qso_database.get_exposure(odometer)
            header_values = exp_status_db_to_header(exposure_status)
            distribute_product(product_file, header_values, self.quicklook)

    @staticmethod
    def __find_nights(night_pattern: str) -> Sequence[str]:
        night_root = Path(PRODUCT_ROOT)
        nights = [str(night.relative_to(night_root)) for night in night_root.glob(night_pattern) if night.is_dir()]
        return sorted(nights)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parsers = {'parser': parser}
    command_parser = parser.add_subparsers(dest='command')
    command_parser.required = True
    qrunid_parser = command_parser.add_parser('qrunid', help='Distribute nights belonging to qrunid')
    qrunid_parser.add_argument('qrunid')
    night_parser = command_parser.add_parser('night', help='Distribute specified night')
    night_parser.add_argument('night')
    file_parser = command_parser.add_parser('file', help='Distribute specified file')
    file_parser.add_argument('night')
    file_parser.add_argument('file')
    command_parser.add_parser('all', help='Distribute all nights')
    parser.add_argument('--quicklook', action='store_true')
    args = parser.parse_args()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    log.addHandler(console_handler)
    log.setLevel(logging.DEBUG)

    distributor = Distributor(args.quicklook)
    if args.command == 'qrunid':
        distributor.distribute_qrun(args.qrunid)
    elif args.command == 'night':
        distributor.distribute_night(args.night)
    elif args.command == 'file':
        distributor.distribute_file(args.night, args.file)
    elif args.command == 'all':
        distributor.distribute_all_nights()
