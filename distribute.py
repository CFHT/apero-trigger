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
from collections.abc import Collection, Iterable, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, Union
from urllib.error import URLError
from urllib.request import Request, urlopen

from astropy.io import fits

log = logging.getLogger()

FitsHeaderValue = Union[str, int, float, complex, bool]
FitsHeaderCard = Union[FitsHeaderValue, tuple[FitsHeaderValue, str]]
FitsHeaderDict = dict[str, FitsHeaderCard]
JsonObj = dict[str, Union[dict, list, str, int, float, bool, None]]

DATA_ROOT = '/data/spirou4/apero-data/'
DISTRIBUTION_ROOT = '/data/distribution/spirou/' # Set to None to update files in place
KEY_FILE = '/h/spirou/bin/.cfht_access'
OFFLINE_PROFILE = 'offline293'
QUICKLOOK_PROFILE = 'quicklook'


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


class Exposure(NamedTuple):
    grade: int
    state: str

    @staticmethod
    def from_exposure_data(exposure_data: JsonObj) -> 'Exposure':
        exposure_status = exposure_data['exposure_status']
        grade = exposure_status.get('grade')
        state = exposure_status['exp_status']
        return Exposure(grade, state)

    def to_header(self) -> FitsHeaderDict:
        return {
            'QSOVALID': (self.state, 'QSO validation state'),
            'QSOGRADE': (self.grade, 'QSO grade (1=good 5=unusable)'),
        }


class ExposureSequence:
    exposures: list[Exposure]

    def __init__(self, exposures):
        self.exposures = exposures

    @staticmethod
    def indexed_header_key(original_key: str, index: int, max_index: int) -> str:
        n_digits = len(str(max_index))
        n_key_chars = min(8 - n_digits, len(original_key))
        return original_key[:n_key_chars] + str(index)

    def to_header(self) -> FitsHeaderDict:
        per_key = defaultdict(OrderedDict)
        for i, exposure in enumerate(self.exposures):
            header_cards = exposure.to_header()
            for key, value in header_cards.items():
                new_key = self.indexed_header_key(key, i + 1, len(self.exposures))
                per_key[key][new_key] = value
        combined = {key: value for current in per_key.values() for key, value in current.items()}
        return combined


class QsoDatabase:
    def __init__(self):
        try:
            with open(KEY_FILE, 'r') as file_read:
                self.bearer_token = file_read.read().strip()
        except OSError:
            log.error('Failed to load API bearer token, will not be able to access database', exc_info=False)
            self.bearer_token = None
        self.cache = dict()

    def fetch_and_cache(self, obsids: Collection[int]) -> dict[int, Exposure]:
        result = self.get_exposures(obsids)
        self.cache.update(result)
        return result

    def get_exposure_lazy(self, obsid: int) -> Exposure:
        return self.cache[obsid]

    def get_exposure_range_lazy(self, first: int, last: int) -> ExposureSequence:
        return ExposureSequence([self.cache[i] for i in range(first, last + 1)])

    def get_exposures(self, obsids: Collection[int]) -> dict[int, Exposure]:
        if not self.bearer_token:
            log.warning('No bearer token loaded, cannot fetch values from the database')
            return {}
        auth_headers = {'Authorization': 'Bearer ' + self.bearer_token}
        url = 'https://api.cfht.hawaii.edu/op/exposures'
        request_data = {
            'obsid_range': {
                'first': min(obsids),
                'last': max(obsids),
            },
            'response_filter': 'SPIROU_HEADERS',
        }
        response = json_request(url, request_data, headers=auth_headers, retries=2)
        return {int(exposure['obsid']): Exposure.from_exposure_data(exposure) for exposure in response['exposure']}


def get_distribution_path(source: Path, run_id: str, distribution_subdirectory: str) -> Path:
    distribution_dir = Path(DISTRIBUTION_ROOT, run_id.lower(), distribution_subdirectory)
    try:
        distribution_dir.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.error('Failed to create distribution directory %s due to', str(distribution_dir), str(err))
    return Path(distribution_dir, source.name)


def distribute_product(product: Path, header_values: FitsHeaderDict, quicklook: bool):
    file_mode = 'readonly' if DISTRIBUTION_ROOT else 'update'
    try:
        with fits.open(product, mode=file_mode) as hdulist:
            hdulist[0].header.update(header_values)
            if DISTRIBUTION_ROOT:
                subdir = 'quicklook' if quicklook else 'reduced'
                run_id = hdulist[0].header['RUNID']
                destination = get_distribution_path(product, run_id, subdir)
                log.info('Distributing %s', destination)
                hdulist.writeto(destination, overwrite=True)
            else:
                log.info('Distributing %s', product)
    except FileNotFoundError as err:
        log.error('Distribution of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Distribution of %s failed', product, exc_info=True)


def extract_odometer(file: Path):
    odometer = int(file.stem[0:-1])
    return odometer


def extract_odometer_ftype(file: Path):
    odometer = int(file.stem[0:-1])
    letter = file.stem[-1]
    return odometer, letter


class Distributor:
    def __init__(self, profile: str, quicklook: bool = False):
        self.product_root = Path(DATA_ROOT, profile, 'out')
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
        night_dir = Path(self.product_root, night)
        products = list(sorted(file for file in night_dir.glob('*.fits') if file.exists()))
        odometers = list(extract_odometer(product) for product in products)
        self.qso_database.cache = dict()
        self.qso_database.fetch_and_cache(odometers)
        for product in products:
            self.distribute_product(product)

    def distribute_file(self, night: str, file: str):
        product_file = Path(self.product_root, night, file)
        odometer = extract_odometer(product_file)
        self.qso_database.fetch_and_cache((odometer,))
        self.distribute_product(product_file)

    def distribute_product(self, product_file: Path):
        odometer, letter = extract_odometer_ftype(product_file)
        if letter == 'p':
            sequence = self.qso_database.get_exposure_range_lazy(odometer, odometer + 3)
            header_values = sequence.to_header()
            distribute_product(product_file, header_values, self.quicklook)
        else:
            exposure = self.qso_database.get_exposure_lazy(odometer)
            header_values = exposure.to_header()
            distribute_product(product_file, header_values, self.quicklook)

    def __find_nights(self, night_pattern: str) -> Sequence[str]:
        night_root = Path(self.product_root)
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

    profile = QUICKLOOK_PROFILE if args.quicklook else OFFLINE_PROFILE
    distributor = Distributor(profile, args.quicklook)
    if args.command == 'qrunid':
        distributor.distribute_qrun(args.qrunid)
    elif args.command == 'night':
        distributor.distribute_night(args.night)
    elif args.command == 'file':
        distributor.distribute_file(args.night, args.file)
    elif args.command == 'all':
        distributor.distribute_all_nights()
