from ..exposureconfig import TargetType, FiberType
from ..steps import ObjectStep


class ObjectProcessor():
    def __init__(self, object_steps, drs, packager, ccf_params):
        self.steps = object_steps
        self.drs = drs
        self.packager = packager
        self.ccf_params = ccf_params

    def process_object_exposure(self, config, exposure):
        extracted_path = self.extract_object(exposure)
        if config.object.target == TargetType.STAR:
            is_telluric_corrected = self.telluric_correction(exposure)
            is_obj_fp = config.object.reference_fiber == FiberType.FP
            ccf_path = self.ccf(exposure, is_telluric_corrected, is_obj_fp)
            if ObjectStep.PRODUCTS in self.steps:
                self.packager.create_1d_spectra_product(exposure, is_telluric_corrected)
            return {
                'extracted_path': extracted_path,
                'ccf_path': ccf_path,
                'is_ccf_calculated': True,
                'is_telluric_corrected': is_telluric_corrected,
            }
        if ObjectStep.PRODUCTS in self.steps:
            self.packager.create_1d_spectra_product(exposure)
        return {'extracted_path': extracted_path}

    def process_polar_seqeunce(self, exposures):
        if len(exposures) < 2:
            return {'is_polar_done': False}
        if ObjectStep.POL in self.steps:
            self.drs.pol(exposures)
        if ObjectStep.PRODUCTS in self.steps:
            self.packager.create_pol_product(exposures[0])
        return {'is_polar_done': True}

    def extract_object(self, exposure):
        if ObjectStep.EXTRACT in self.steps:
            self.drs.cal_extract_RAW(exposure)
        if ObjectStep.PRODUCTS in self.steps:
            self.packager.create_2d_spectra_product(exposure)
        return exposure.e2ds('AB')

    def telluric_correction(self, exposure):
        if ObjectStep.FITTELLU in self.steps:
            try:
                result = self.drs.obj_fit_tellu(exposure)
                telluric_corrected = bool(result)
            except:
                telluric_corrected = False
        else:
            expected_telluric_path = exposure.e2ds('AB', telluric_corrected=True, flat_fielded=True)
            telluric_corrected = expected_telluric_path.exists()
        if ObjectStep.MKTEMPLATE in self.steps:
            self.drs.obj_mk_template(exposure)
        if ObjectStep.PRODUCTS in self.steps:
            self.packager.create_tell_product(exposure)
        return telluric_corrected

    def ccf(self, exposure, telluric_corrected, fp):
        if ObjectStep.CCF in self.steps:
            self.drs.cal_CCF_E2DS(exposure, self.ccf_params, telluric_corrected, fp)
        mask = self.ccf_params.mask
        if ObjectStep.PRODUCTS in self.steps:
            self.packager.create_ccf_product(exposure, mask, telluric_corrected=telluric_corrected, fp=fp)
        return exposure.ccf('AB', mask, telluric_corrected=telluric_corrected, fp=fp)
