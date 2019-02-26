import os
import pathlib
import shutil
from threading import Thread

from astropy.io import fits


def distribute_file(file, subdir, nonblocking=True):
    hdu = fits.open(file)[0]
    run_id = hdu.header['RUNID']
    dist_root = '/data/distribution/spirou/'
    dist_dir = os.path.join(dist_root, run_id.lower(), subdir)
    pathlib.Path(dist_dir).mkdir(parents=True, exist_ok=True)
    if nonblocking:
        Thread(target=shutil.copy2, args=[file, dist_dir]).start()
        return os.path.join(dist_dir, os.path.basename(file))
    else:
        new_file = shutil.copy2(file, dist_dir)
        return new_file
