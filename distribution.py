import shutil
from pathlib import Path
from threading import Thread

from astropy.io import fits

distribution_root = '/data/distribution/spirou/'

def distribute_file(path, distribution_subdirectory, nonblocking=True):
    hdu = fits.open(path)[0]
    run_id = hdu.header['RUNID']
    distribution_dir = Path(distribution_root, run_id.lower(), distribution_subdirectory)
    distribution_dir.mkdir(parents=True, exist_ok=True)
    if nonblocking:
        Thread(target=shutil.copy2, args=[path, distribution_dir]).start()
        return distribution_dir.joinpath(path.name)
    else:
        new_file = shutil.copy2(path, distribution_dir)
        return Path(new_file)
