from collections import Iterable, namedtuple
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


CcfParams = namedtuple('CcfParams', ('mask', 'v0', 'range', 'step'))

class DRS:
    def __init__(self, trace=False, realtime=False):
        self.trace = trace
        self.realtime = realtime

    def cal_preprocess(self, exposure):
        return self.__logwrapper(cal_preprocess_spirou, exposure.night, exposure.raw.name)

    def cal_extract_RAW(self, exposure):
        return self.__logwrapper(cal_extract_RAW_spirou, exposure.night, exposure.preprocessed.name)

    def cal_DARK(self, exposures):
        return self.__sequence_logwrapper(cal_DARK_spirou, exposures)

    def cal_BADPIX(self, flat_exposure, dark_exposure):
        flat_file = flat_exposure.preprocessed.name
        dark_file = dark_exposure.preprocessed.name
        return self.__logwrapper(cal_BADPIX_spirou, flat_exposure.night, flat_file, dark_file)

    def cal_loc_RAW(self, exposures):
        return self.__sequence_logwrapper(cal_loc_RAW_spirou, exposures)

    def cal_FF_RAW(self, exposures):
        return self.__sequence_logwrapper(cal_FF_RAW_spirou, exposures)

    def cal_SLIT(self, exposures):
        return self.__sequence_logwrapper(cal_SLIT_spirou, exposures)

    def cal_SHAPE(self, hc_exposure, fp_exposures):
        night = hc_exposure.night
        hc_file = hc_exposure.preprocessed.name
        fp_files = [fp_exposure.preprocessed.name for fp_exposure in fp_exposures]
        return self.__logwrapper(cal_SHAPE_spirou, night, hc_file, fp_files)

    def cal_HC_E2DS(self, exposure, fiber):
        file = exposure.e2ds(fiber).name
        return self.__logwrapper(cal_HC_E2DS_EA_spirou, exposure.night, file)

    def cal_WAVE_E2DS(self, fp_exposure, hc_exposure, fiber):
        hc_file = hc_exposure.e2ds(fiber).name
        fp_file = fp_exposure.e2ds(fiber).name
        return self.__logwrapper(cal_WAVE_E2DS_EA_spirou, hc_exposure.night, fp_file, [hc_file])

    def cal_CCF_E2DS(self, exposure, params, telluric_corrected, fp):
        file = exposure.e2ds('AB', telluric_corrected, flat_fielded=True).name
        ccf_recipe = cal_CCF_E2DS_FP_spirou if fp else cal_CCF_E2DS_spirou
        return self.__logwrapper(ccf_recipe, exposure.night, file, params.mask, params.v0, params.range, params.step)

    def pol(self, exposures):
        input_files = [exposure.e2ds(fiber).name for exposure in exposures for fiber in ('A', 'B')]
        return self.__logwrapper(pol_spirou, exposures[0].night, input_files)

    def obj_mk_tellu(self, exposure):
        return self.__logwrapper(obj_mk_tellu, exposure.night, [exposure.e2ds('AB', flat_fielded=True).name])

    def obj_fit_tellu(self, exposure):
        return self.__logwrapper(obj_fit_tellu, exposure.night, [exposure.e2ds('AB', flat_fielded=True).name])

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
                if self.__should_log_errors_to_director(module):
                    director_message('DRS command failed (exit): ' + command_string, level='warning')
            except QCFailure:
                logger.error('QC failed for DRS recipe: %s', command_string)
                if self.__should_log_errors_to_director(module):
                    director_message('DRS QC failed for command: ' + command_string, level='warning')
            except Exception as error:
                logger.error('DRS recipe failed with uncaught exception: %s', command_string, exc_info=True)
                if self.__should_log_errors_to_director(module):
                    director_message('DRS command failed (exception): ' + command_string, level='warning')

    def __sequence_logwrapper(self, module, exposures):
        return self.__logwrapper(module, exposures[0].night, [exposure.preprocessed.name for exposure in exposures])

    def __should_log_errors_to_director(self, module):
        ignore_modules = (cal_CCF_E2DS_spirou, cal_CCF_E2DS_FP_spirou, obj_mk_tellu, obj_fit_tellu, pol_spirou)
        return self.realtime and module not in ignore_modules
