#!/usr/bin/env python
from setuptools import setup

setup(
    name='hydra-pywr',
    version='0.1',
    description='Hydra plugins and library for import and exporting Pywr models.',
    packages=['hydra_pywr'],
    package_data={
        # Include all the gzipped model
        'hydra_pywr': ['node_layouts.json', 'pywr-app-service.yml'],
    },
    entry_points='''
    [console_scripts]
    hydra-pywr=hydra_pywr.cli:start_cli
    ''',
)
