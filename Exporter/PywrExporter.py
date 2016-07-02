import sys
import os
import time
import json
from datetime import datetime

from string import ascii_lowercase

from HydraLib.PluginLib import JSONPlugin
from HydraLib.HydraException import HydraPluginError
from HydraLib.hydra_dateutil import reindex_timeseries
from HydraLib import PluginLib

from HydraLib.PluginLib import write_progress, write_output
from PywrJsonWriter import pywrwriter

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a network from Hydra to a Pywr Json text file.
                    (c) Copyright 2016, Univeristy of Manchester.
        """, epilog="For more information, web site will available soon",
        formatter_class=ap.RawDescriptionHelpFormatter)

    parser.add_argument('-t', '--network-id',
                        help='''ID of the network that will be exported.''')
    parser.add_argument('-s', '--scenario-id',
                        help='''ID of the scenario that will be exported.''')

    parser.add_argument('-tp', '--template-id',
                        help='''ID of the template to be used.''')

    parser.add_argument('-o', '--output',
                        help='''Json Output file containing exported data''')

    parser.add_argument('-u', '--server-url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')

    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser

from decimal import Decimal

import json

import logging
import argparse as ap

log = logging.getLogger(__name__)

def check_args(args):
    try:
        int(args.network_id)
    except (TypeError, ValueError):
        raise HydraPluginError('No network is specified')
    try:
        int(args.scenario_id)
    except (TypeError, ValueError):
        raise HydraPluginError('No senario is specified')

    output = os.path.dirname(args.output)
    if output == '':
        output = '.'

    if  os.path.exists(output)==False:
        raise HydraPluginError('Output file directory '+
                               os.path.dirname(args.output)+
                               'does not exist')

class PywrExporter(JSONPlugin):
    def __init__(self, args):
        self.connect(args)
        self.network_id = int(args.network_id)
        self.scenario_id = int(args.scenario_id)
        self.template_id = int(args.template_id) if args.template_id is not None else None
        self.net = self.connection.call('get_network', {'network_id': self.network_id,
                                                   'include_data': 'Y',
                                                   'template_id': self.template_id,
                                                   'scenario_ids': [self.scenario_id]})
        self.attrlist = self.connection.call('get_all_attributes', {})
if __name__ == '__main__':
    errors  = []
    steps=7
    steps=7
    try:
        write_progress(1, steps)
        parser = commandline_parser()
        args = parser.parse_args()
        check_args(args)
        pywrexporter=PywrExporter(args)
        pywrwriter(pywrexporter.net, pywrexporter.attrlist, args.output)
    except HydraPluginError, e:
        write_progress(steps, steps)
        log.exception(e)
        errors = [e.message]
    except Exception, e:
        log.exception(e)
        errors = []
        if e.message == '':
            if hasattr(e, 'strerror'):
                errors = [e.strerror]
        else:
            errors = [e.message]
