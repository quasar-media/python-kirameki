import warnings

try:
    from kirameki._version import version as __version__
except ImportError:
    warnings.warn(
        "your install of kirameki is broken; chances are you didn't "
        "have setuptools-scm while installing the package in development"
    )
    __version__ = "0.0.0"

__all__ = ["__version__"]
