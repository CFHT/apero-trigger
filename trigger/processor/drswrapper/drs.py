from typing import Sequence

from apero.recipes.spirou import cal_badpix_spirou, cal_ccf_spirou, cal_extract_spirou, cal_flat_spirou, \
    cal_leak_spirou, cal_loc_spirou, cal_preprocess_spirou, cal_shape_spirou, cal_thermal_spirou, \
    cal_wave_night_spirou, obj_fit_tellu_spirou

from .reciperunner import RecipeRunner
from ...baseinterface.processor import IErrorHandler
from ...common.drsconstants import Fiber
from ...common.pathhandler import Exposure, TelluSuffix

try:
    import sys
    from pathlib import Path
    from apero.core import constants
    from logger import log
    config = constants.load('SPIROU')
    polar_root = Path(config['DRS_ROOT']).parent.parent.joinpath('spirou-polarimetry')
    sys.path.append(str(polar_root))
    import spirou_pol
except ModuleNotFoundError:
    spirou_pol = None
    log.warning('Failed to import spirou_pol, polarimetry recipe will not be able to run')


class DRS:
    def __init__(self, trace=False, log_command=True, error_handler: IErrorHandler = None):
        self.runner = RecipeRunner(trace=trace, log_command=log_command, error_handler=error_handler)

    @property
    def trace(self):
        return self.runner.trace

    def cal_preprocess(self, exposure: Exposure) -> bool:
        """
        :param exposure: Any exposure
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(cal_preprocess_spirou, exposure.night, exposure.raw.name)

    def cal_badpix(self, flat_exposures: Sequence[Exposure], dark_exposures: Sequence[Exposure]) -> bool:
        """
        :param flat_exposures: FLAT_FLAT sequence
        :param dark_exposures: DARK_DARK_INT or DARK_DARK_TEL sequence
        :return: Whether the recipe completed successfully
        """
        flat_files = [flat.preprocessed.name for flat in flat_exposures]
        dark_files = [dark.preprocessed.name for dark in dark_exposures]
        return self.runner.run(cal_badpix_spirou, flat_exposures[0].night, flat_files, dark_files)

    def cal_loc(self, exposures: Sequence[Exposure]) -> bool:
        """
        :param exposures: FLAT_DARK or DARK_FLAT sequence
        :return: Whether the recipe completed successfully
        """
        return self.__run_sequence(cal_loc_spirou, exposures)

    def cal_shape(self, exposures: Sequence[Exposure]) -> bool:
        """
        :param exposures: FP_FP sequence
        :return: Whether the recipe completed successfully
        """
        return self.__run_sequence(cal_shape_spirou, exposures)

    def cal_flat(self, exposures: Sequence[Exposure]) -> bool:
        """
        :param exposures: FLAT_FLAT sequence 
        :return: Whether the recipe completed successfully
        """
        return self.__run_sequence(cal_flat_spirou, exposures)

    def cal_thermal(self, exposures: Sequence[Exposure]) -> bool:
        """
        :param exposures: DARK_DARK_INT or DARK_DARK_TEL sequence
        :return: Whether the recipe completed successfully
        """
        return self.__run_sequence(cal_thermal_spirou, exposures)

    def cal_wave(self, hc_exposures, fp_exposures) -> bool:
        """
        :param hc_exposures: HCONE_HCONE sequence
        :param fp_exposures: FP_FP sequence
        :return: Whether the recipe completed successfully
        """
        hc_files = [flat.preprocessed.name for flat in hc_exposures]
        fp_files = [dark.preprocessed.name for dark in fp_exposures]
        return self.runner.run(cal_wave_night_spirou, hc_exposures[0].night, hc_files, fp_files)

    def cal_extract(self, exposure: Exposure, **kwargs) -> bool:
        """
        :param exposure: Any exposure that has been preprocessed
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(cal_extract_spirou, exposure.night, exposure.preprocessed.name, **kwargs)

    def cal_leak(self, exposure: Exposure) -> bool:
        """
        :param exposure: OBJ_FP exposure that has been extracted
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(cal_leak_spirou, exposure.night, exposure.e2ds(Fiber.AB).name)

    def obj_fit_tellu(self, exposure: Exposure) -> bool:
        """
        :param exposure: OBJ_DARK or OBJ_FP exposure that has been extracted
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(obj_fit_tellu_spirou, exposure.night, exposure.e2ds(Fiber.AB).name)

    def cal_ccf(self, exposure: Exposure, telluric_corrected=True) -> bool:
        """
        :param exposure: OBJ_DARK or OBJ_FP exposure that has been extracted
        :param telluric_corrected: Whether to use telluric corrected e2ds
        :return: Whether the recipe completed successfully
        """
        file = exposure.e2ds(Fiber.AB, TelluSuffix.tcorr(telluric_corrected)).name
        return self.runner.run(cal_ccf_spirou, exposure.night, file)

    def pol(self, exposures: Sequence[Exposure]) -> bool:
        """
        :param exposures OBJ_* sequence that has been extracted
        :return: Whether the recipe completed successfully
        """
        if spirou_pol is None:
            return False
        input_files = (str(exposure.final_product('e')) for exposure in exposures)
        return self.runner.run(spirou_pol, *input_files, output=str(exposures[0].final_product('p')))

    def __run_sequence(self, module, exposures: Sequence[Exposure]) -> bool:
        return self.runner.run(module, exposures[0].night, [exposure.preprocessed.name for exposure in exposures])
