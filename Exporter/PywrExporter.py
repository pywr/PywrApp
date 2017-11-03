# (c) Copyright 2017 University of Manchester\

"""A Hydra plug-in to export a network and a scenario to a set of files, which
can be imported into a JSON Pywr model.

It creates the Pywr JSON file from Hydra network
More information about pywr Json format can be found in this link:
https://pywr.github.io/pywr-docs/json.html

The Pywr Emporter plug-in provides an easy to use tool for exporting data from
HydraPlatform to JSON model format. The basic idea is that this plug-in
exports a network and associated data from HydraPlatform to a text file which
can be loaded into Pywr model using model=Model.load(json_file_name)

Using the commandline tool
--------------------------

**arguments:**

====================== ======= ========== ======================================
Option                 Short   Parameter  Description
====================== ======= ========== ======================================
--network              -t      NETWORK    ID of the network that will be
                                          exported.
--scenario             -s      SCENARIO   ID of the scenario that will be
                                          exported.
--template-id          -tp     TEMPLATE   ID of the template used for exporting
                                          resources. Attributes that don't
                                          belong to this template are ignored.
--output               -o      OUTPUT     Filename of the output file. if is not provided
                                          a "network_network_id.json" will be used as a default output file name
====================== ======= ========== ======================================

**Server-based arguments**
====================== ====== ========== =========================================
Option                 Short  Parameter  Description
====================== ====== ========== =========================================
--server_url           -u     SERVER_URL Url of the server the plugin will
                                         connect to.
                                         Defaults to localhost.
--session_id           -c     SESSION_ID Session ID used by the calling software
                                         If left empty, the plugin will attempt


The main goal of this plug-in is to provide a tool for exporting
network topologies and data to a file readable by Pywr.

Examples:
=========
Exporting use time axis:
 python GAMSExport.py -t 16 -s 16  -o "c:\temp\example_2.json"

"""

import os
from HydraLib.PluginLib import JSONPlugin
from HydraLib.HydraException import HydraPluginError
from HydraLib import PluginLib

from HydraLib.PluginLib import write_progress, write_output
from PywrJsonWriter import pywrwriter

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a network from Hydra to a Pywr Json text file.
                    (c) Copyright 2017, Univeristy of Manchester.
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

    output = args.output
    if output!=None:
        if  os.path.exists(args.output)==False:
            raise HydraPluginError('Output file directory '+
                               os.path.dirname(args.output)+
                               'does not exist')
    else:
        args.output = "network_" + args.network_id + ".json"

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
        self.template = self.connection.call('get_template',
                      {'template_id': self.net.types[0].template_id})


if __name__ == '__main__':
    errors  = []
    steps=7
    try:
        write_progress(1, steps)
        parser = commandline_parser()
        args = parser.parse_args()
        check_args(args)
        write_progress(2, steps)
        pywrexporter=PywrExporter(args)
        write_progress(3, steps)
        pywrwriter(pywrexporter.net, pywrexporter.attrlist, args.output, steps)

        text = PluginLib.create_xml_response('Pywr Exporter', (args.network_id), [args.scenario_id],
                                             message="Data export was successful.",
                                             errors=errors)
    except HydraPluginError, e:
        write_progress(steps, steps)
        log.exception(e)
        errors = [e.message]
        text = PluginLib.create_xml_response('Pywr Exporter', (args.network_id), [args.scenario_id],
                                             message="Error while exporting data.",
                                             errors=errors)
    except Exception, e:
        log.exception(e)
        errors = []
        if e.message == '':
            if hasattr(e, 'strerror'):
                errors = [e.strerror]
        else:
            errors = [e.message]
        text = PluginLib.create_xml_response('Pywr Exporter', (args.network_id), [args.scenario_id],
                                             message="Error while exporting data.",
                                             errors=errors)
    write_progress(steps, steps)
    print text





