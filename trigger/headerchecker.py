from typing import Tuple

from logger import log
from .baseinterface.headerchecker import DateTime, HeaderChecker


class SpirouHeaderChecker(HeaderChecker):
    MIN_EXP_TIME_RATIO_THRESHOLD = 0.1

    def is_object(self) -> bool:
        return self.header.get('OBSTYPE') == 'OBJECT'

    def get_object_name(self) -> str:
        name_keyword = 'OBJECT'
        if name_keyword not in self.header:
            name_keyword = 'OBJNAME'
            if name_keyword not in self.header:
                raise RuntimeError('Object file missing OBJECT and OBJNAME keywords', self.file)
        return self.header[name_keyword]

    def is_sky(self) -> bool:
        object_name = self.get_object_name()
        return (self.header.get('TRGTYPE') == 'SKY'
                or object_name.lower() == 'sky'
                or object_name.startswith('sky_')
                or object_name.endswith('_sky'))

    def get_exposure_index_and_total(self) -> Tuple[int, int]:
        if 'CMPLTEXP' not in self.header or 'NEXP' not in self.header:
            log.warning('%s missing CMPLTEXP/NEXP in header, treating sequence as single exposure', self.file)
            return 1, 1
        else:
            return self.header['CMPLTEXP'], self.header['NEXP']

    def get_rhomb_positions(self) -> Tuple[str, str]:
        if 'SBRHB1_P' not in self.header:
            raise RuntimeError('Object file missing SBRHB1_P keyword', self.file)
        if 'SBRHB2_P' not in self.header:
            raise RuntimeError('Object file missing SBRHB2_P keyword', self.file)
        return self.header.get('SBRHB1_P'), self.header.get('SBRHB2_P')

    def get_obs_date(self) -> DateTime:
        return self.header.get('MJDATE')

    def get_runid(self) -> str:
        return self.header.get('RUNID')

    def is_aborted(self) -> bool:
        if 'EXPTIME' not in self.header or 'EXPREQ' not in self.header:
            log.warning('%s missing EXPTIME/EXPREQ in header, assuming not an aborted exposure', self.file)
            return False
        return self.header['EXPTIME'] / self.header['EXPREQ'] < self.MIN_EXP_TIME_RATIO_THRESHOLD
