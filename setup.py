import sys
from os import path

from setuptools import find_packages, setup

PROJECT_ROOT = path.abspath(path.dirname(__file__))
SOURCE_ROOT = path.join(PROJECT_ROOT, "src")

sys.path.insert(0, SOURCE_ROOT)

from kirameki import __about__  # noqa isort:skip

install_requires = ["psycopg2 ~= 2.8.6"]

extras_require = {
    "test": [
        "pytest == 6.2.3",
        "pytest-cov == 2.11.1",
        "pytest-mock == 3.6.1",
    ],
    "docs": ["Sphinx == 4.0.2", "sphinx-rtd-theme"],
    "flask": ["Flask"],
}

setup(
    name="kirameki",
    version=__about__.__version__,
    author="Auri",
    author_email="me@aurieh.me",
    url="https://github.com/quasar-media/kirameki",
    install_requires=install_requires,
    extras_require=extras_require,
    package_dir={"": "src"},
    packages=find_packages(where=SOURCE_ROOT),
    include_package_data=True,
    classifiers=[
        "Development Status :: 1 - Planning",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Topic :: Database",
    ],
)
