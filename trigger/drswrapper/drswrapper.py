import cal_BADPIX_spirou
import cal_CCF_E2DS_FP_spirou
import cal_CCF_E2DS_spirou
import cal_DARK_spirou
import cal_FF_RAW_spirou
import cal_HC_E2DS_EA_spirou
import cal_SHAPE_spirou
import cal_WAVE_E2DS_EA_spirou
import cal_extract_RAW_spirou
import cal_loc_RAW_spirou
import cal_preprocess_spirou
import obj_fit_tellu
import obj_mk_obj_template
import obj_mk_tellu_db
import pol_spirou

from .reciperunner import RecipeRunner


class DRS:
    def __init__(self, trace=False, log_command=True, error_handler=None):
        self.runner = RecipeRunner(trace=trace, log_command=log_command, error_handler=error_handler)

    def cal_preprocess(self, exposure):
        return self.runner.run(cal_preprocess_spirou, exposure.night, exposure.raw.name)

    def cal_extract_RAW(self, exposure):
        return self.runner.run(cal_extract_RAW_spirou, exposure.night, exposure.preprocessed.name)

    def cal_DARK(self, exposures):
        return self.__run_sequence(cal_DARK_spirou, exposures)

    def cal_BADPIX(self, flat_exposure, dark_exposure):
        flat_file = flat_exposure.preprocessed.name
        dark_file = dark_exposure.preprocessed.name
        return self.runner.run(cal_BADPIX_spirou, flat_exposure.night, flat_file, dark_file)

    def cal_loc_RAW(self, exposures):
        return self.__run_sequence(cal_loc_RAW_spirou, exposures)

    def cal_FF_RAW(self, exposures):
        return self.__run_sequence(cal_FF_RAW_spirou, exposures)

    def cal_SHAPE(self, hc_exposure, fp_exposures):
        night = hc_exposure.night
        hc_file = hc_exposure.preprocessed.name
        fp_files = [fp_exposure.preprocessed.name for fp_exposure in fp_exposures]
        return self.runner.run(cal_SHAPE_spirou, night, hc_file, fp_files)

    def cal_HC_E2DS(self, exposure, fiber):
        file = exposure.e2ds(fiber).name
        return self.runner.run(cal_HC_E2DS_EA_spirou, exposure.night, file)

    def cal_WAVE_E2DS(self, fp_exposure, hc_exposure, fiber):
        hc_file = hc_exposure.e2ds(fiber).name
        fp_file = fp_exposure.e2ds(fiber).name
        return self.runner.run(cal_WAVE_E2DS_EA_spirou, hc_exposure.night, fp_file, [hc_file])

    def cal_CCF_E2DS(self, exposure, params, telluric_corrected, fp):
        file = exposure.e2ds('AB', telluric_corrected, flat_fielded=True).name
        ccf_recipe = cal_CCF_E2DS_FP_spirou if fp else cal_CCF_E2DS_spirou
        return self.runner.run(ccf_recipe, exposure.night, file, params.mask, params.v0, params.range, params.step)

    def pol(self, exposures):
        input_files = [exposure.e2ds(fiber).name for exposure in exposures for fiber in ('A', 'B')]
        return self.runner.run(pol_spirou, exposures[0].night, input_files)

    def obj_fit_tellu(self, exposure):
        return self.runner.run(obj_fit_tellu, exposure.night, [exposure.e2ds('AB', flat_fielded=True).name])

    def obj_mk_template(self, exposure):
        return self.runner.run(obj_mk_obj_template, exposure.night, [exposure.e2ds('AB', flat_fielded=True).name])

    def obj_mk_tellu(self):
        return self.runner.run(obj_mk_tellu_db)

    def __run_sequence(self, module, exposures):
        return self.runner.run(module, exposures[0].night, [exposure.preprocessed.name for exposure in exposures])
