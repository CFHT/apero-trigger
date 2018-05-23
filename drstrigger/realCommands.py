from cal_preprocess_spirou import main as cal_preprocess_spirou

from cal_extract_RAW_spirou import main as cal_extract_RAW_spirou
# (night_name=None, files=None, fiber_type=None, **kwargs)

# TODO: uncomment below once other recipes work with odometer filenames
'''from cal_DARK_spirou import main as cal_DARK_spirou
from cal_loc_RAW_spirou import main as cal_loc_RAW_spirou
from cal_FF_RAW_spirou import main as cal_FF_RAW_spirou
from cal_SLIT_spirou import main as cal_SLIT_spirou
# (night_name=None, files=None)

from cal_WAVE_E2DS_spirou import main as cal_WAVE_E2DS_spirou
# (night_name=None, hcfiles=None, fpfile=None)

from cal_DRIFTPEAK_E2DS_spirou import main as cal_DRIFTPEAK_E2DS_spirou
# (night_name=None, reffile=None)
'''
# TODO: delete below once above is uncommented
from .dummyCommands import cal_DARK_spirou
from .dummyCommands import cal_loc_RAW_spirou
from .dummyCommands import cal_FF_RAW_spirou
from .dummyCommands import cal_SLIT_spirou
from .dummyCommands import cal_WAVE_E2DS_spirou
from .dummyCommands import cal_DRIFTPEAK_E2DS_spirou


# TODO: delete below once sure it's not needed
'''from fileproccesser import blocking_subprocess


def recipe_on_file(recipe, night, file):
    bindir = '/data/spirou/spirou-drs/INTROOT/bin/'
    interpreter = '/data/spirou/venv/bin/python3'
    python_paths = ['.', '/data/spirou/spirou-drs/INTROOT', '/data/spirou/spirou-drs/INTROOT/bin']
    python_path_str = ':'.join(python_paths)
    blocking_subprocess('PYTHONPATH="' + python_path_str + '" ' + bindir  + recipe, [night, file], interpreter)


def cal_preprocess_spirou(night_name, file):
    recipe_on_file('cal_preprocess_spirou.py', night_name, file)
    return file.replace('.fits', '_pp.fits')

def cal_DARK_spirou(night_name, file):
    recipe_on_file('cal_DARK_spirou.py', night_name, file)


def cal_loc_RAW_spirou(night_name, file):
    recipe_on_file('cal_loc_RAW_spirou.py', night_name, file)


def cal_FF_RAW_spirou(night_name, file):
    recipe_on_file('cal_FF_RAW_spirou.py', night_name, file)


def cal_SLIT_spirou(night_name, file):
    recipe_on_file('cal_SLIT_spirou.py', night_name, file)


def cal_WAVE_E2DS_spirou(night_name, file):
    recipe_on_file('cal_WAVE_E2DS_spirou.py', night_name, file)


def cal_DRIFTPEAK_E2DS_spirou(night_name, file):
    recipe_on_file('cal_DRIFTPEAK_E2DS_spirou.py', night_name, file)


def cal_extract_RAW_spirou(night_name, file):
    recipe_on_file('cal_extract_RAW_spirou.py', night_name, file)
'''