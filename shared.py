import os
from astropy.io import fits
from fileproccesser import blocking_subprocess

input_directory = '/data/spirou2/raw/'
reduced_directory = '/data/spirou1/reduced/'
bin_dir = '/data/spirou/trigger/'

env = os.environ.copy()

def set_drs_config_subdir(subdirectory):
    set_drs_config_dir(os.path.join(bin_dir, subdirectory))

def set_drs_config_dir(directory):
    env['DRS_UCONFIG'] = directory

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
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger.py'), [night, '--file', file], env=env)

def process_sequence(night, files):
    print("Spirou DRS processing sequence:", ', '.join(files))
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger.py'), [night, '--sequence'] + files, env=env)
