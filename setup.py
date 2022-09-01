#!/usr/bin/env python3

import re

from pkg_resources import parse_requirements
from setuptools import find_packages, setup

PACKAGE_NAME = "multinet"

dependencies = []
with open("requirements/production.txt") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        match = re.search(r"(\S+)==(\d+)\.(\d+)\.(\d+)", line)
        if not match: 
            continue

        pkg, major, minor, patch = match.groups()
        major = int(major)
        minor = int(minor)
        patch = int(patch)
        dependencies.append(f"{pkg}>={major}.{minor}.{patch},<{major+1}")

setup(
    name=PACKAGE_NAME,
    use_scm_version=dict(write_to=f"{PACKAGE_NAME}/version.py"),
    packages=find_packages(),
    install_requires=dependencies,
    setup_requires=["setuptools_scm"],
)

