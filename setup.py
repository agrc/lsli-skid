#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
setup.py
A module that installs the lsli skid as a module
"""

from pathlib import Path

from setuptools import find_packages, setup

#: Load version from source file
version = {}
version_file = Path(__file__).parent / "src" / "lsli" / "version.py"
exec(version_file.read_text(), version)

setup(
    name="lsli",
    version=version["__version__"],
    license="MIT",
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="UGRC",
    author_email="ugrc-developers@utah.gov",
    url="https://github.com/agrc/lsli",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Utilities",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/agrc/lsli/issues",
    },
    keywords=["gis"],
    install_requires=[
        "ugrc-palletjack>=5.0,<5.2",
        "ugrc-supervisor==3.*",
        "gql==3.5.*",
    ],
    extras_require={
        "tests": [
            "pytest-cov>=3,<7",
            "pytest-instafail==0.5.*",
            "pytest-mock==3.*",
            "pytest-watch==4.*",
            "pytest>=6,<9",
            "ruff==0.*",
        ]
    },
    setup_requires=[
        "pytest-runner",
    ],
    entry_points={
        "console_scripts": [
            "lsli = lsli.main:process",
        ]
    },
)
