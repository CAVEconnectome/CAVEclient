import importlib.metadata

__version__ = importlib.metadata.version("CAVEclient")

from .frameworkclient import CAVEclient
from .session_config import get_session_defaults, set_session_defaults

__all__ = ["CAVEclient", "set_session_defaults", "get_session_defaults"]
