import shutil
from pathlib import Path
from threading import Thread

from astropy.io import fits

from trigger import log
from .dbinterface import DatabaseHeaderConverter

distribution_root = '/data/distribution/spirou/'


def get_distribution_path(source, run_id, distribution_subdirectory):
    distribution_dir = Path(distribution_root, run_id.lower(), distribution_subdirectory)
    try:
        distribution_dir.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.error('Failed to create distribution directory %s due to', str(distribution_dir), str(err))
    return Path(distribution_dir, source.name)


def try_copy(src, dest):
    try:
        return shutil.copy2(src, dest)
    except OSError as err:
        log.error('Distribution of %s failed due to %s ', str(src), str(err))
    return dest


def distribute_raw_file(path, nonblocking=True):
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
    def __init__(self, trace, realtime, distribute):
        self.realtime = realtime
        self.distribute = distribute and not trace

    def get_exposure_distributor(self, exposure_status):
        return ExposureProductDistributor(self.distribute, self.realtime, exposure_status)

    def get_seqeunce_distributor(self, exposure_statuses):
        return SequenceProductDistributor(self.distribute, self.realtime, exposure_statuses)


class ProductDistributor:
    def __init__(self, distribute, quicklook):
        self.distribute = distribute
        self.quicklook = quicklook
        self.header_values = {}

    def distribute_product(self, exposure, product_letter):
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
            except:
                log.error('Distribution of %s failed', file, exc_info=True)
            else:
                log.info('Distributing %s', destination)


class ExposureProductDistributor(ProductDistributor):
    def __init__(self, distribute, quicklook, exposure_status):
        super().__init__(distribute, quicklook)
        if exposure_status:
            self.header_values = DatabaseHeaderConverter.exp_status_db_to_header(exposure_status)


class SequenceProductDistributor(ProductDistributor):
    def __init__(self, distribute, quicklook, exposure_statuses):
        super().__init__(distribute, quicklook)
        if exposure_statuses:
            self.header_values = DatabaseHeaderConverter.seq_status_db_to_header(exposure_statuses)
