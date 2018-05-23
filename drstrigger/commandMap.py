from . import realCommands
from .exposureConfigMapper import ExposureTemplate, ConfigCommandMap


class CommandMap(ConfigCommandMap):
    def __init__(self):
        super(CommandMap, self).__init__()
        self.add(ActualTemplates.dark(), realCommands.cal_DARK_spirou)
        self.add(ActualTemplates.dark_long(), realCommands.cal_DARK_spirou)
        self.add(ActualTemplates.loc_ab(), realCommands.cal_loc_RAW_spirou)
        self.add(ActualTemplates.loc_c(), realCommands.cal_loc_RAW_spirou)
        self.add(ActualTemplates.flat(), realCommands.cal_FF_RAW_spirou)
        self.add(ActualTemplates.slit(), realCommands.cal_SLIT_spirou)
        self.add(ActualTemplates.wave1(), realCommands.cal_WAVE_E2DS_spirou)
        self.add(ActualTemplates.wave2(), realCommands.cal_WAVE_E2DS_spirou)
        self.add(ActualTemplates.fp1(), realCommands.cal_DRIFTPEAK_E2DS_spirou)
        self.add(ActualTemplates.fp2(), realCommands.cal_DRIFTPEAK_E2DS_spirou)
        self.add(ActualTemplates.star(), realCommands.cal_extract_RAW_spirou)
        self.add(ActualTemplates.star_wave(), realCommands.cal_extract_RAW_spirou)
        self.add(ActualTemplates.star_fp(), realCommands.cal_extract_RAW_spirou)
        self.add(ActualTemplates.pol(), realCommands.cal_extract_RAW_spirou)
        self.add(ActualTemplates.pol_wave(), realCommands.cal_extract_RAW_spirou)
        self.add(ActualTemplates.pol_fp(), realCommands.cal_extract_RAW_spirou)
        # self.add(pol_eng, None) TODO


class ActualTemplates(ExposureTemplate):
    @classmethod
    def dark(cls):
        return cls.calibration2(refout='pos_pk', cassout='pos_pk', nexp='*', exptime='<1800')

    @classmethod
    def dark_long(cls):
        return cls.calibration2(refout='pos_pk', cassout='pos_pk', nexp='*', exptime='>=1800')

    @classmethod
    def loc_ab(cls):
        return cls.calibration2(refout='pos_wl', cassout='pos_pk', nexp='=1', exptime='*')

    @classmethod
    def loc_c(cls):
        return cls.calibration2(refout='pos_pk', cassout='pos_wl', nexp='=1', exptime='*')

    @classmethod
    def flat(cls):
        # workaround since there is no NEXP header for calibrations TODO
        return cls.calibration2(refout='pos_wl', cassout='pos_wl', nexp='=1', exptime='*')
        # return cls.calibration2(refout='pos_wl', cassout='pos_wl', nexp='>5', exptime='*')

    @classmethod
    def slit(cls):
        return cls.calibration2(refout='pos_fp', cassout='pos_fp', nexp='=1', exptime='*')

    @classmethod
    def wave1(cls):
        return cls.calibration16(refout='pos_pk', cassout='pos_hc1|pos_hc2', nexp='=1', exptime='*')

    @classmethod
    def wave2(cls):
        return cls.calibration16(refout='pos_hc1|pos_hc2', cassout='pos_pk', nexp='=1', exptime='*')

    @classmethod
    def fp1(cls):
        return cls.calibration16(refout='pos_fp', cassout='pos_fp', nexp='>2', exptime='*')

    @classmethod
    def fp2(cls):
        return cls.calibration16(refout='pos_fp', cassout='pos_fp', nexp='=1', exptime='*')

    @classmethod
    def wave_fp(cls):
        return cls.calibration16(refout='pos_hc1|pos_hc2', cassout='pos_fp', nexp='>2', exptime='*')

    @classmethod
    def fp_wave(cls):
        return cls.calibration16(refout='pos_fp', cassout='pos_hc1|pos_hc2', nexp='=1', exptime='*')

    @classmethod
    def star(cls):
        return cls.object_star(refout='pos_pk')

    @classmethod
    def star_wave(cls):
        return cls.object_star(refout='pos_hc1|pos_hc2')

    @classmethod
    def star_fp(cls):
        return cls.object_star(refout='pos_fp')

    @classmethod
    def pol(cls):
        return cls.object_pol(refout='pos_pk')

    @classmethod
    def pol_wave(cls):
        return cls.object_pol(refout='pos_hc1|pos_hc2')

    @classmethod
    def pol_fp(cls):
        return cls.object_pol(refout='pos_fp')

    @classmethod
    def pol_eng(cls):
        return cls.pol().copy_and_set(nexp='=64', exptime='*')

    @classmethod
    def calibration2(cls, refout, cassout, nexp, exptime):
        return cls.calibration(rhomb2='P2', refout=refout, cassout=cassout, nexp=nexp, exptime=exptime)

    @classmethod
    def calibration16(cls, refout, cassout, nexp, exptime):
        return cls.calibration(rhomb2='P16', refout=refout, cassout=cassout, nexp=nexp, exptime=exptime)

    @classmethod
    def calibration(cls, rhomb2, refout, cassout, nexp, exptime):
        return cls.exposure(calibwh='P4', rhomb1='P16', rhomb2=rhomb2, refout=refout, cassout=cassout, nexp=nexp, exptime=exptime)

    @classmethod
    def object_star(cls, refout):
        return cls.object(rhomb='P16', refout=refout, nexp='=1')

    @classmethod
    def object_pol(cls, refout):
        # workaround for missing NEXP header TODO
        return cls.object(rhomb='P2|P4|P14|P16', refout=refout, nexp='*')
        # return cls.object(rhomb='P2|P4|P14|P16', refout=refout, nexp='=4')

    @classmethod
    def object(cls, rhomb, refout, nexp):
        return cls.exposure(calibwh='P5', rhomb1=rhomb, rhomb2=rhomb, refout=refout, cassout='pos_pk', nexp=nexp, exptime='10-1800')

    @classmethod
    def exposure(cls, calibwh, rhomb1, rhomb2, refout, cassout, nexp, exptime):
        return ExposureTemplate.of(
            calibwh=calibwh,
            rhomb1=rhomb1,
            rhomb2=rhomb2,
            refout=refout,
            cassout=cassout,
            nexp=nexp,
            exptime=exptime)
