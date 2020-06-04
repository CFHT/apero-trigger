class DrsLoader:
    BIN_DIR = '/data/spirou/trigger/'
    SESSION_DIR = '/data/sessions/spirou/'

    @staticmethod
    def set_drs_config_subdir(subdirectory: str):
        import os
        os.environ['DRS_UCONFIG'] = os.path.join(DrsLoader.BIN_DIR, subdirectory)

    def __init__(self):
        import cfht
        self.cfht = cfht

    def get_loaded_trigger_module(self):
        return self.cfht
