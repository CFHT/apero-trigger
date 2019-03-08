from collections import Iterable
import sys
from logger import logger, director_message

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
import cal_CCF_E2DS_FP_spirou
import pol_spirou
import obj_mk_tellu
import obj_fit_tellu
from SpirouDRS import spirouConfig
import cal_DRIFTPEAK_E2DS_spirou

FIBER_LIST = ('AB', 'A', 'B', 'C')


def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x


class QCFailure(Exception):
    def __init__(self, errors):
        super().__init__('QC failure: ' + ', '.join(errors))
        self.errors = errors


class DRS:
    def __init__(self, trace=False, realtime=False):
        self.trace = trace
        self.realtime = realtime

    def cal_preprocess(self, path):
        return self.__logwrapper(cal_preprocess_spirou, path.night, path.raw.filename)

    def cal_extract_RAW(self, path):
        return self.__logwrapper(cal_extract_RAW_spirou, path.night, path.preprocessed.filename)

    def cal_DARK(self, paths):
        return self.__sequence_logwrapper(cal_DARK_spirou, paths)

    def cal_BADPIX(self, flat_path, dark_path):
        flat_file = flat_path.preprocessed.filename
        dark_file = dark_path.preprocessed.filename
        return self.__logwrapper(cal_BADPIX_spirou, flat_path.night, flat_file, dark_file)

    def cal_loc_RAW(self, paths):
        return self.__sequence_logwrapper(cal_loc_RAW_spirou, paths)

    def cal_FF_RAW(self, paths):
        return self.__sequence_logwrapper(cal_FF_RAW_spirou, paths)

    def cal_SLIT(self, paths):
        return self.__sequence_logwrapper(cal_SLIT_spirou, paths)

    def cal_SHAPE(self, hc_path, fp_paths):
        night = hc_path.night
        hc_file = hc_path.preprocessed.filename
        fp_files = [fp_path.preprocessed.filename for fp_path in fp_paths]
        return self.__logwrapper(cal_SHAPE_spirou, night, hc_file, fp_files)

    def cal_HC_E2DS(self, path, fiber):
        file = path.e2ds(fiber).filename
        return self.__logwrapper(cal_HC_E2DS_EA_spirou, path.night, file)

    def cal_WAVE_E2DS(self, fp_path, hc_path, fiber):
        hc_file = hc_path.e2ds(fiber).filename
        fp_file = fp_path.e2ds(fiber).filename
        return self.__logwrapper(cal_WAVE_E2DS_EA_spirou, hc_path.night, fp_file, [hc_file])

    def cal_CCF_E2DS(self, path, mask, telluric_corrected, fp):
        filename = path.e2ds('AB', telluric_corrected, flat_fielded=True).filename
        ccf_recipe = cal_CCF_E2DS_FP_spirou if fp else cal_CCF_E2DS_spirou
        return self.__logwrapper(ccf_recipe, path.night, filename, mask, 0, 100, 1)

    def pol(self, paths):
        input_files = [path.e2ds(fiber).filename for path in paths for fiber in ('A', 'B')]
        return self.__logwrapper(pol_spirou, paths[0].night, input_files)

    def obj_mk_tellu(self, path):
        return self.__logwrapper(obj_mk_tellu, path.night, [path.e2ds('AB', flat_fielded=True).filename])

    def obj_fit_tellu(self, path):
        return self.__logwrapper(obj_fit_tellu, path.night, [path.e2ds('AB', flat_fielded=True).filename])

    @staticmethod
    def version():
        return spirouConfig.Constants.VERSION()

    def __logwrapper(self, module, night, *args):
        command_string = ' '.join((module.__NAME__, night, *map(str, flatten(args))))
        logger.info(command_string)
        if self.trace:
            return True
        else:
            sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
            try:
                locals = module.main(night, *args)
                qc_passed = locals.get('passed')
                qc_failures = locals.get('fail_msg')
                if qc_failures and not qc_passed:
                    raise QCFailure(qc_failures)
                return True
            except SystemExit:
                logger.error('DRS recipe failed with a system exit: %s', command_string)
                if self.realtime:
                    director_message('DRS command failed (exit): ' + command_string, level='warning')
            except QCFailure:
                logger.error('QC failed for DRS recipe: %s', command_string)
                if self.realtime:
                    director_message('DRS QC failed for command: ' + command_string, level='warning')
            except Exception as error:
                logger.error('DRS recipe failed with uncaught exception: %s', command_string, exc_info=True)
                if self.realtime:
                    director_message('DRS command failed (exception): ' + command_string, level='warning')

    def __sequence_logwrapper(self, module, paths):
        return self.__logwrapper(module, paths[0].night, [path.preprocessed.filename for path in paths])
