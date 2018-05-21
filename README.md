# PywrApp

Import from, export to and run Pywr models from Hydra Platform.

## Installation

This app is written as a conventional Python package and can therefore be installed 
like any other. 

`python setup.py install`

There are several dependencies that are required. The most complex of which is Pywr
and most be installed in the same Python environment from which the app is to be
executed.

## Command line interface

Installation registers the app with the executable `hydra-pywr`. This executable
provides access to several commands facilitating interaction between Pywr and Hydra.
These commands are also made accessible to Hydra through `plugin.xml`. These are
registered through the `register` command:

`hydra-pywr register`


### Registering the template

The PywrApp can also generate a Hydra template from the currently installed version 
of `pywr`. This template can be registered with hydra using the `template` sub-command
`register` as follows:

`hydra-pywr template register`

