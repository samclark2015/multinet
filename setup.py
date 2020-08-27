#!/usr/bin/env python3

from setuptools import setup, find_packages
from pip._internal.req import parse_requirements
from pip._internal.download import PipSession

requirements = parse_requirements("requirements/production.txt", session=PipSession())

PACKAGE_NAME = "multinet"

dependencies = [
    # 'numpy',
    *[str(req.req) for req in requirements]
]

setup(
    name=PACKAGE_NAME,
    use_scm_version=dict(write_to=f"{PACKAGE_NAME}/version.py"),
    packages=find_packages(),
    install_requires=dependencies,
    setup_requires=["setuptools_scm"],
)

