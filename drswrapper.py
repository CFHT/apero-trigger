from collections import Iterable

import envloader # Setup Python path to include DRS paths

import cal_preprocess_spirou
import cal_extract_RAW_spirou
import cal_DARK_spirou
import cal_BADPIX_spirou
import cal_loc_RAW_spirou
import cal_FF_RAW_spirou
import cal_SLIT_spirou
import cal_WAVE_E2DS_spirou
import cal_CCF_E2DS_spirou
import pol_spirou
import obj_mk_tellu
import obj_fit_tellu
from SpirouDRS import spirouConfig
import cal_HC_E2DS_spirou
import cal_DRIFTPEAK_E2DS_spirou


def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x


def logwrapper(module, night, *args):
    print_args = flatten(args)
    print(module.__NAME__, night, *print_args)
    try:
        return module.main(night, *args)
    except SystemExit:
        print('DRS recipe', module.__NAME__, 'failed')


def sequence_logwrapper(module, paths):
    return logwrapper(module, paths[0].night(), [path.preprocessed_filename() for path in paths])


class DRS:
    @staticmethod
    def cal_preprocess(path):
        return logwrapper(cal_preprocess_spirou, path.night(), path.raw_filename())

    @staticmethod
    def cal_extract_RAW(path):
        return logwrapper(cal_extract_RAW_spirou, path.night(), path.preprocessed_filename())

    @staticmethod
    def cal_DARK(paths):
        return sequence_logwrapper(cal_DARK_spirou, paths)

    @staticmethod
    def cal_BADPIX(flat_path, dark_path):
        flat_file = flat_path.preprocessed_filename()
        dark_file = dark_path.preprocessed_filename()
        return logwrapper(cal_BADPIX_spirou, flat_path.night(), flat_file, dark_file)

    @staticmethod
    def cal_loc_RAW(paths):
        return sequence_logwrapper(cal_loc_RAW_spirou, paths)

    @staticmethod
    def cal_FF_RAW(paths):
        return sequence_logwrapper(cal_FF_RAW_spirou, paths)

    @staticmethod
    def cal_SLIT(paths):
        return sequence_logwrapper(cal_SLIT_spirou, paths)

    @staticmethod
    def cal_WAVE_E2DS(fp_path, hc_path, fiber):
        hc_file = hc_path.e2ds_filename(fiber)
        fp_file = fp_path.e2ds_filename(fiber)
        return logwrapper(cal_WAVE_E2DS_spirou, hc_path.night(), fp_file, [hc_file])

    @staticmethod
    def cal_CCF_E2DS(path, telluric_corrected=False):
        filename = path.telluric_corrected_filename('AB') if telluric_corrected else path.e2ds_filename('AB')
        return logwrapper(cal_CCF_E2DS_spirou, path.night(), filename, 'gl581_Sep18_cleaned.mas', 0, 100, 1)

    @staticmethod
    def pol(paths):
        input_files = [path.e2ds_filename(fiber) for path in paths for fiber in ('A', 'B')]
        return logwrapper(pol_spirou, paths[0].night(), input_files)

    @staticmethod
    def obj_mk_tellu(path):
        return logwrapper(obj_mk_tellu, path.night(), [path.e2ds_filename('AB')])

    @staticmethod
    def obj_fit_tellu(path):
        return logwrapper(obj_fit_tellu, path.night(), [path.e2ds_filename('AB')])

    @staticmethod
    def version():
        return spirouConfig.Constants.VERSION()
