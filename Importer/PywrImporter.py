# (c) Copyright 2017 University of Manchester\

'''
 plugin_name: PywrImporter

Basics
~~~~~~
The Pywr import plug-in provides an easy to use tool to import JSON model format
into HydraPlatform network.

This App reads JSON file model file and create Hydra network and pushed to the server

More information about pywr Json format can be found in this link:
https://pywr.github.io/pywr-docs/json.html

**Argument:**


====================== ====== ========== ======================================
Option                 Short  Parameter  Description
====================== ====== ========== ======================================
                                         used for the simulation.
--JSON-model-format-file            -f     Json file containing Pywr model

**Server-based arguments**
====================== ====== ========== =========================================
Option                 Short  Parameter  Description
====================== ====== ========== =========================================
--server_url           -u     SERVER_URL Url of the server the plugin will
                                         connect to.
                                         Defaults to localhost.
--session_id           -c     SESSION_ID Session ID used by the calling software
                                         If left empty, the plugin will attempt

Examples:
=========
python PywrImporter.py  -m "c:\temp\Example_3.json"
'''

from HydraLib.PluginLib import JSONPlugin
from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output
from PywrJsonReader import import_net, add_network
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

  #  try:
    parser = commandline_parser()
    write_progress(2, steps)


    args = parser.parse_args()
    write_progress(3, steps)

    connector=HydraConnector(args)
    c_attrlist = connector.connection.call('get_all_attributes', {})
    import json
    #print "c_attrlist:", json.dumps(c_attrlist)
    write_progress(4, steps)

    hydra_network, nodes_types, links_types=import_net(args.json_file, c_attrlist, connector.connection)
    network =add_network(hydra_network, connector.connection,nodes_types, links_types)
    write_progress(5, steps)

    text = PluginLib.create_xml_response('Pywr importer', (network.id), [network.scenarios[0].id],
                                         message="Data import was successful.",
                                         errors=[])
  #  except Exception, e:
   # error = [e.message]
   # text = PluginLib.create_xml_response('Pywr importer', "", "", message="Error while importing data",errors=error)

    write_progress(6, steps)

    print text
