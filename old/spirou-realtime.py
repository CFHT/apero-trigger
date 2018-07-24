#!/data/spirou/venv/bin/python3

import time, os, glob
from fileproccesser import blocking_subprocess
from astropy.io import fits

link_root = '/data/manao/spirou/'
link_directory = link_root + 'spirou/'
bin_dir = '/data/spirou/realtime/'

def move_to_night_dir(file, night):
    new_directory = link_root + night + '/'
    if not os.path.exists(new_directory):
        os.makedirs(new_directory)
    new_name = new_directory + os.path.basename(file)
    os.rename(file, new_name)
    return new_name

def get_night(filename):
    try:
        header = fits.open(filename)[0].header
        return header['DATE-OBS']
    except:
        print("Couldn't determine night for " + filename)
        raise Exception

env = os.environ.copy()
env['DRS_UCONFIG'] = '/data/spirou/realtime/'

while True:
    all_files = sorted(glob.glob(link_directory + '*.fits'), key=os.path.getmtime)
    for file in all_files:
        ppfile = file.replace('.fits', '_pp.fits')
        if not file.endswith(('g.fits', 'r.fits', 'RW.fits')):
            print("Spirou Realtime processing file:", file)
            night = get_night(file)
            file = move_to_night_dir(file, night)
            ppfile = file.replace('.fits', '_pp.fits')
            blocking_subprocess(bin_dir + 'preprocess-wrapper.py', [night, file], env=env)

            blocking_subprocess(bin_dir + 'drstrigger.py', [night, ppfile], env=env)
        else:
            print("Spirou Realtime skipping file:", file)
        os.remove(file)
        if os.path.isfile(ppfile):
            os.remove(ppfile)
    time.sleep(1)

# echo "@say_ status: test spirou realtime status" | nc -q 0 spirou-session 20140
