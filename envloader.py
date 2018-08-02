import sys
from envconfig import drs_root

PYTHONPATHS = [drs_root, drs_root + '/bin']
sys.path.extend(PYTHONPATHS)
from SpirouDRS import spirouConfig

config, _warnings = spirouConfig.ReadConfigFile()

input_root_directory = config['DRS_DATA_RAW']
reduced_root_directory = config['DRS_DATA_REDUC']
