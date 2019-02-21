import os
import pathlib
import shutil

from astropy.io import fits


def distribute_file(file, subdir):
    hdu = fits.open(file)[0]
    run_id = hdu.header['RUNID']
    dist_root = '/data/distribution/spirou/'
    dist_dir = os.path.join(dist_root, run_id.lower(), subdir)
    pathlib.Path(dist_dir).mkdir(parents=True, exist_ok=True)
    new_file = shutil.copy2(file, dist_dir)
    return new_file

