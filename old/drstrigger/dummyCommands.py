import inspect


def cal_DARK_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_loc_RAW_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_FF_RAW_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_SLIT_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_WAVE_E2DS_spirou(night_name, hcfile):
    dummy_function(night_name, hcfile)


def cal_DRIFTPEAK_E2DS_spirou(night_name, reffile):
    dummy_function(night_name, reffile)


def cal_HC_E2DS_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_extract_RAW_spirou(night_name, files):
    dummy_function(night_name, files)


def cal_preprocess_spirou(night_name, files):
    dummy_function(night_name, files)


def dummy_function(night_name, file_name):
    print("DUMMY COMMAND - Night:", night_name, "-", "Running", inspect.stack()[1][3], "on", file_name)
