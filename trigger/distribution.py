import shutil
from pathlib import Path
from threading import Thread

from astropy.io import fits

from logger import log
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
