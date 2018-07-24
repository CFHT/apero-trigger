#!/data/spirou/venv/bin/python3

import sys, os
PYTHONPATHS = ['/data/spirou/spirou-drs/INTROOT', '/data/spirou/spirou-drs/INTROOT/bin']
sys.path.extend(PYTHONPATHS)

import argparse
from drstrigger import ActualExposureConfig, CommandMap, MissingKeysError, OpeningFITSError, UnknownConfigError

def main(args):
    night = args.night
    filename = args.filename
    command_map = CommandMap()
    try:
        exposure_config = ActualExposureConfig.from_file(filename)
        drs_command = command_map.get(exposure_config)
        filename = os.path.basename(filename)
        sys.argv = [sys.argv[0]]
        result = drs_command(night, files=[filename])
    except OpeningFITSError:
        print('Failed to open FITS header of', filename)
    except MissingKeysError as e:
        print('Header of', filename, 'missing keyword(s):', ','.join(e.keys))
    except UnknownConfigError:
        print('Failed to find matching DRS command to run on', filename)
    except Exception as e:
        print('Error running selected DRS command on', filename)
        print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    parser.add_argument('filename')
    args = parser.parse_args()
    main(args)
