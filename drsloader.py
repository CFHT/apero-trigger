class DrsLoader:
    BIN_DIR = '/data/spirou/trigger/'
    DRS_ROOT = '/data/spirou/spirou-drs/INTROOT'
    SESSION_DIR = '/data/sessions/spirou/'

    @staticmethod
    def set_drs_config_subdir(subdirectory):
        import os
        os.environ['DRS_UCONFIG'] = os.path.join(DrsLoader.BIN_DIR, 'config', subdirectory)

    def __init__(self):
        import sys
        PYTHONPATHS = [DrsLoader.DRS_ROOT, DrsLoader.DRS_ROOT + '/bin']
        sys.path.extend(PYTHONPATHS)
        import cfht
        self.cfht = cfht

    def get_loaded_trigger_module(self):
        return self.cfht
