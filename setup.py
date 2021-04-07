from os import path
from setuptools import find_packages, setup

PROJECT_ROOT = path.abspath(path.dirname(__file__))
SOURCE_ROOT = path.join(PROJECT_ROOT, "src")

setup(
    name="kirameki",
    use_scm_version=True,
    author="Auri",
    author_email="me@aurieh.me",
    url="https://github.com/quasar-media/kirameki",
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
