#!/usr/bin/env python
from setuptools import setup

setup(
    name='hydra-pywr',
    version='0.1',
    description='Hydra plugins and library for import and exporting Pywr models.',
    packages=['hydra_pywr'],
    include_package_data=True,
    entry_points='''
    [console_scripts]
    hydra-pywr=hydra_pywr.cli:start_cli
    ''',
)
