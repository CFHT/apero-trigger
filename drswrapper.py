from collections import Iterable
import sys
from logger import logger

import envloader # Setup Python path to include DRS paths

import cal_preprocess_spirou
import cal_extract_RAW_spirou
import cal_DARK_spirou
import cal_BADPIX_spirou
import cal_loc_RAW_spirou
import cal_FF_RAW_spirou
import cal_SLIT_spirou
import cal_SHAPE_spirou
import cal_HC_E2DS_EA_spirou
import cal_WAVE_E2DS_EA_spirou
import cal_CCF_E2DS_spirou
import pol_spirou
import obj_mk_tellu
import obj_fit_tellu
from SpirouDRS import spirouConfig
import cal_DRIFTPEAK_E2DS_spirou


def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x


class DRS:
    def __init__(self, trace=False):
        self.trace = trace

    def cal_preprocess(self, path):
        return self.__logwrapper(cal_preprocess_spirou, path.night(), path.raw_filename())

    def cal_extract_RAW(self, path):
        return self.__logwrapper(cal_extract_RAW_spirou, path.night(), path.preprocessed_filename())

    def cal_DARK(self, paths):
        return self.__sequence_logwrapper(cal_DARK_spirou, paths)

    def cal_BADPIX(self, flat_path, dark_path):
        flat_file = flat_path.preprocessed_filename()
        dark_file = dark_path.preprocessed_filename()
        return self.__logwrapper(cal_BADPIX_spirou, flat_path.night(), flat_file, dark_file)

    def cal_loc_RAW(self, paths):
        return self.__sequence_logwrapper(cal_loc_RAW_spirou, paths)

    def cal_FF_RAW(self, paths):
        return self.__sequence_logwrapper(cal_FF_RAW_spirou, paths)

    def cal_SLIT(self, paths):
        return self.__sequence_logwrapper(cal_SLIT_spirou, paths)

    def cal_SHAPE(self, paths):
        return self.__sequence_logwrapper(cal_SHAPE_spirou, paths)

    def cal_HC_E2DS(self, path, fiber):
        file = path.e2ds_filename(fiber)
        return self.__logwrapper(cal_HC_E2DS_EA_spirou, path.night(), file)

    def cal_WAVE_E2DS(self, fp_path, hc_path, fiber):
        hc_file = hc_path.e2ds_filename(fiber)
        fp_file = fp_path.e2ds_filename(fiber)
        return self.__logwrapper(cal_WAVE_E2DS_EA_spirou, hc_path.night(), fp_file, [hc_file])

    def cal_CCF_E2DS(self, path, mask, telluric_corrected):
        filename = path.e2ds_filename('AB', telluric_corrected)
        return self.__logwrapper(cal_CCF_E2DS_spirou, path.night(), filename, mask, 0, 100, 1)

    def pol(self, paths):
        input_files = [path.e2ds_filename(fiber) for path in paths for fiber in ('A', 'B')]
        return self.__logwrapper(pol_spirou, paths[0].night(), input_files)

    def obj_mk_tellu(self, path):
        return self.__logwrapper(obj_mk_tellu, path.night(), [path.e2ds_filename('AB')])

    def obj_fit_tellu(self, path):
        return self.__logwrapper(obj_fit_tellu, path.night(), [path.e2ds_filename('AB')])

    @staticmethod
    def version():
        return spirouConfig.Constants.VERSION()

    def __logwrapper(self, module, night, *args):
        command_string = ' '.join((module.__NAME__, night, *map(str, flatten(args))))
        logger.info(command_string)
        if not self.trace:
            sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
            try:
                return module.main(night, *args)
            except SystemExit:
                logger.error('DRS recipe %s failed with a system exit', module.__NAME__)
            except Exception as error:
                logger.error('DRS recipe %s failed with uncaught exception', module.__NAME__, exc_info=True)

    def __sequence_logwrapper(self, module, paths):
        return self.__logwrapper(module, paths[0].night(), [path.preprocessed_filename() for path in paths])
