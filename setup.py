#!/usr/bin/env python3

from setuptools import setup, find_packages


PACKAGE_NAME = "multinet"

dependencies = [
    # 'numpy',
    "cad_io"
]

setup(
    name=PACKAGE_NAME,
    use_scm_version=dict(write_to=f"{PACKAGE_NAME}/version.py"),
    packages=find_packages(),
    install_requires=dependencies,
    setup_requires=["setuptools_scm"],
)
