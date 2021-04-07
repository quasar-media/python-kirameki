# flake8: noqa:F401
import warnings

from kirameki import _priority, exc
from kirameki._priority import *
from kirameki.exc import *

try:
    from kirameki._version import version as __version__
except ImportError:
    warnings.warn(
        "your install of kirameki is broken; chances are you didn't "
        "have setuptools-scm while installing the package in development"
    )
    __version__ = "0.0.0"

__all__ = ["__version__"]
__all__.extend(_priority.__all__)
__all__.extend(exc.__all__)
