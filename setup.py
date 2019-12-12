#!/usr/bin/env python3
from setuptools import setup, find_packages

requirements = None
with open('requirements.txt') as req_txt:
    requirements = req_txt.readlines()

setup(
    name='multinet',
    version=1.0,
    packages=find_packages(exclude=("tests",)),
    install_requires=requirements
)
