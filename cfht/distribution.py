import shutil
from pathlib import Path
from threading import Thread
from typing import Collection

from astropy.io import fits

from logger import log
from trigger.common import Exposure
from .dbinterface import DatabaseHeaderConverter, JsonObj
from .pathconfig import DISTRIBUTION_ROOT


def get_distribution_path(source: Path, run_id: str, distribution_subdirectory: str) -> Path:
    distribution_dir = Path(DISTRIBUTION_ROOT, run_id.lower(), distribution_subdirectory)
    try:
        distribution_dir.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.error('Failed to create distribution directory %s due to', str(distribution_dir), str(err))
    return Path(distribution_dir, source.name)


def try_copy(src: Path, dest: Path) -> Path:
    try:
        return shutil.copy2(src, dest)
    except OSError as err:
        log.error('Distribution of %s failed due to %s ', str(src), str(err))
    return dest


def distribute_raw_file(path: Path, nonblocking=True) -> Path:
    hdulist = fits.open(path)
    run_id = hdulist[0].header['RUNID']
    destination = get_distribution_path(path, run_id, 'raw')
    log.info('Distributing %s', destination)
    if nonblocking:
        Thread(target=try_copy, args=[path, destination]).start()
        return destination
    else:
        new_file = try_copy(path, destination)
        return Path(new_file)


class ProductDistributorFactory:
    def __init__(self, quicklook: bool, distribute: bool):
        self.quicklook = quicklook
        self.distribute = distribute

    def get_exposure_distributor(self, exposure_status):
        return ExposureProductDistributor(self.distribute, self.quicklook, exposure_status)

    def get_sequence_distributor(self, exposure_statuses):
        return SequenceProductDistributor(self.distribute, self.quicklook, exposure_statuses)


class ProductDistributor:
    def __init__(self, distribute: bool, quicklook: bool):
        self.distribute = distribute
        self.quicklook = quicklook
        self.header_values = {}

    def distribute_product(self, exposure: Exposure, product_letter: str):
        if self.distribute:
            subdir = 'quicklook' if self.quicklook else 'reduced'
            file = exposure.final_product(product_letter)
            try:
                hdulist = fits.open(file)
                run_id = hdulist[0].header['RUNID']
                hdulist[0].header.update(self.header_values)
                destination = get_distribution_path(file, run_id, subdir)
                hdulist.writeto(destination, overwrite=True)
            except FileNotFoundError as err:
                log.error('Distribution of %s failed: unable to open file %s', file, err.filename)
            except Exception:
                log.error('Distribution of %s failed', file, exc_info=True)
            else:
                log.info('Distributing %s', destination)


class ExposureProductDistributor(ProductDistributor):
    def __init__(self, distribute: bool, quicklook: bool, exposure_status: JsonObj):
        super().__init__(distribute, quicklook)
        if exposure_status:
            self.header_values = DatabaseHeaderConverter.exp_status_db_to_header(exposure_status)


class SequenceProductDistributor(ProductDistributor):
    def __init__(self, distribute: bool, quicklook: bool, exposure_statuses: Collection[JsonObj]):
        super().__init__(distribute, quicklook)
        if exposure_statuses:
            self.header_values = DatabaseHeaderConverter.seq_status_db_to_header(exposure_statuses)
