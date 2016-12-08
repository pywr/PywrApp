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
from PywrJsonReader import import_net




from decimal import Decimal

import json

import logging
import argparse as ap

log = logging.getLogger(__name__)

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a Pywr Json to Hydra.
                    (c) Copyright 2016, Univeristy of Manchester.
        """, epilog="For more information, web site will available soon",
        formatter_class=ap.RawDescriptionHelpFormatter)

    parser.add_argument('-f', '--json_file',
                        help='file containing pywr json.')

    parser.add_argument('-u', '--server-url',
                        help='''Specify the URL of the server to which this
                            plug-in connects.''')

    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                            attempted based on details in config.''')

    return parser


class HydraConnector(JSONPlugin):
    def __init__(self, args):
        self.connect(args)
        if self.connection is None:
            self.connect(args)


if __name__ == '__main__':
    message=""
    steps=6
    write_progress(1, steps)

    #try:
    parser = commandline_parser()
    write_progress(2, steps)


    args = parser.parse_args()
    write_progress(3, steps)

    connector=HydraConnector(args)
    write_progress(4, steps)

    network=import_net(args.json_file, connector.connection)
    write_progress(5, steps)

    text = PluginLib.create_xml_response('Pywr importer', (network.id), [network.scenarios[0].id],
                                         message="Data import was successful.",
                                         errors=[])
#except Exception, e:
        #error = [e.message]
        #text = PluginLib.create_xml_response('Pywr importer', "", "", message="Error while importing data",errors=error)

    write_progress(6, steps)

    print text
