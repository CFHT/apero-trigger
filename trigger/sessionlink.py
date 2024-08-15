from pathlib import Path

from logger import log
from trigger.common import Exposure
from .pathconfig import SESSION_ROOT


def setup_symlink(session_path: Path) -> Exposure:
    exposure = Exposure.from_path(session_path, custom_raw_root=Path(SESSION_ROOT))
    link_path = exposure.raw
    try:
        link_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.error('Failed to create night directory %s due to %s', str(link_path.parent), str(err))
    else:
        try:
            link_path.symlink_to(session_path)
        except FileExistsError:
            pass
        except OSError as err:
            log.error('Failed to create symlink %s due to %s', str(link_path), str(err))
    return exposure
