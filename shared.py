import os
from astropy.io import fits
from fileproccesser import blocking_subprocess

input_directory = '/data/spirou2/raw/'
bin_dir = '/data/spirou/realtime/'

env = os.environ.copy()
# env['DRS_UCONFIG'] = '/data/spirou/realtime/' # figure out new config options for this

def sequence_runner(current_sequence, file, night):
    if not file.endswith(('g.fits', 'r.fits', 'RW.fits', 'pp.fits')):
        hdu = fits.open(file)[0].header
        exp_index = hdu['CMPLTEXP']
        exp_total = hdu['NEXP']
        process_file(night, file)
        current_sequence.append(file)
        if exp_index == exp_total:
            process_sequence(night, current_sequence)
            current_sequence = []
    else:
        print("Spirou DRS skipping file:", file)
    return current_sequence

def process_file(night, file):
    print("Spirou DRS processing file:", file)
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger-new.py'), [night, '--file', file], env=env)

def process_sequence(night, files):
    print("Spirou DRS processing sequence:", ', '.join(files))
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger-new.py'), [night, '--sequence'] + files, env=env)
