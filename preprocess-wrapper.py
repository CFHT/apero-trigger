#!/data/spirou/venv/bin/python3

import sys, os
PYTHONPATHS = ['/data/spirou/spirou-drs/INTROOT', '/data/spirou/spirou-drs/INTROOT/bin']
sys.path.extend(PYTHONPATHS)

import argparse
from drstrigger.drsCommands import cal_preprocess_spirou

def main(args):
    night = args.night
    filename = args.filename
    try:
        sys.argv = [sys.argv[0]]
        filename = os.path.basename(filename)
        cal_preprocess_spirou(night, filename)
        print('Finished pre-processing on', filename)
    except Exception as e:
        print('Error running selected DRS command on', filename)
        print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    parser.add_argument('filename')
    args = parser.parse_args()
    main(args)
