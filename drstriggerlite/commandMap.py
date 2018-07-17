from . import drsCommands

class CommandMap(object):
    def __init__(self):
        self.data = {}
        self.data['DARK_DARK'] = drsCommands.cal_DARK_spirou
        self.data['DARK_FLAT'] = drsCommands.cal_loc_RAW_spirou
        self.data['FLAT_DARK'] = drsCommands.cal_loc_RAW_spirou
        self.data['FLAT_FLAT'] = drsCommands.cal_FF_RAW_spirou
        self.data['FP_FP'] = drsCommands.cal_SLIT_spirou
        self.data['HCONE_HCONE'] = drsCommands.cal_HC_E2DS_spirou
        self.data['OBJ_OBJ'] = drsCommands.cal_extract_RAW_spirou

    def get(self, configuration):
        if configuration in self.data:
            return self.data[configuration]
        raise UnknownConfigError(configuration)

class UnknownConfigError(Exception):
    def __init__(self, config):
        self.config = config
