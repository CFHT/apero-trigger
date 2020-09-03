import os
from pathlib import Path
from types import ModuleType
from typing import Optional


class DrsLoader:
    CONFIG_ROOT = '/data/spirou/apero/config/'

    def __init__(self, config_subdirectory: Optional[str] = None):
        if config_subdirectory is not None:
            os.environ['DRS_UCONFIG'] = os.path.join(DrsLoader.CONFIG_ROOT, config_subdirectory)
        self.__config_path = os.environ['DRS_UCONFIG']
        import cfht
        self.cfht = cfht

    @property
    def config_path(self) -> Optional[Path]:
        if self.__config_path:
            return Path(self.__config_path)
        return None

    def get_loaded_trigger_module(self) -> ModuleType('cfht'):
        return self.cfht
