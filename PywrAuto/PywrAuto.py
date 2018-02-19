'''
(c) 2017 University of Manchester\

plugin_name: PywrAuto

This app export the Hydranetwork into Pywr json file, runs he Pywr model and extractz the model output and pushes them to the Hydra

**Args:**
====================== ======= ========== =========================================
Option                 Short   Parameter  Description
====================== ======= ========== =========================================
--network              -t      NETWORK    ID of the network where results will
                                          be imported to.
--scenario             -s      SCENARIO   ID of the underlying scenario used for
--template-id          -tp     TEMPLATE   ID of the template used for exporting
                                          resources. Attributes that don't
                                          belong to this template are ignored.
--output               -o      OUTPUT     Filename of the output file.
                                          if is not provided
                                          a "network_network_id.json" will be used as a default output file name
--python3-path         -p     path to     python 3 is required to run pywr
                              python 3
**Server-based arguments**
====================== ====== ========== =========================================
Option                 Short  Parameter  Description
====================== ====== ========== =========================================
--server_url           -u     SERVER_URL Url of the server the plugin will
                                         connect to.
                                         Defaults to localhost.
--session_id           -c     SESSION_ID Session ID used by the calling software
                                         If left empty, the plugin will attempt

Example:
=========
       Python PywrAuto -t 16 -s 16 -p "C:\ProgramData\Anaconda3\envs\pywr"
'''

import os
from datetime import datetime
from dateutil import parser as prs
from dateutil.relativedelta import relativedelta
import subprocess
from HydraLib.PluginLib import JSONPlugin
from HydraLib.HydraException import HydraPluginError
from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output
from Lib.data_files_reader import get_h5DF_store, get_node_attr_values
from Exporter.PywrJsonWriter import pywrwriter
from Exporter.PywrJsonWriter import get_resourcescenarios_ids
from Lib.utilities import  check_output_file

import json
import logging
import argparse as ap
log = logging.getLogger(__name__)

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

    parser.add_argument('-p', '--python3-path',
                        help='''path to python 3''')


    return parser

def check_args(args):
    if args.python3_path==None:
        raise HydraPluginError('No path to python3 is specified, it is required to run pywr')

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
        output = "network_" + args.network_id + ".json"
    return output


class resource_varaiable(object):
    def __init__(self, rec_name, res, value, start_date, end_date, time_step):
        self.values = {}
        start_date=prs.parse(start_date)
        end_date =prs.parse(end_date)
        self.rec_name = rec_name
        self.res=res
        self.value=value
        self.unit='-'
        self.type="timeseries"
        t=time_step.split(' ')
        time_axis=[]
        if t[1]=='days':
            dlt=int(t[0])
            c_time=(start_date)
            time_axis.append(str(c_time))
            while (c_time <end_date):
                c_time= c_time + relativedelta(days=dlt)
                time_axis.append(str(c_time))

        elif t[1]=='months':
            dlt = int(t[0])
            c_time = (start_date)
            time_axis.append(str(c_time))
            while (c_time < end_date):
                c_time = c_time + relativedelta(months=dlt)
                time_axis.append(str(c_time))
        elif t[1] == 'weeks':
            dlt = int(t[0])
            c_time = (start_date)
            time_axis.append(str(c_time))
            while (c_time < end_date):
                c_time = c_time + relativedelta(days=dlt*7)
                time_axis.append(str(c_time))
        if(len(value)==len (time_axis)):
            for i in range (0, len(time_axis)):
                self.values[(time_axis[i])]=float(value[i])


def import_results(results_file, network, nodes_vars):
    csvfile=None
    varaiables_records=[]
    with open(results_file, 'r') as res:
        results = res.read()
    lines = results.split('\n')
    for i in range(0, len(lines)):
        if lines[i]=='':
            continue
        line = lines[i].split(',')
        if line[0] == "start_date":
            start_date=line[1]
        elif line[0]=="end_date":
            end_date=line[1]
        elif line[0]=="timeStep":
            timeStep=line[1]
        else:
            rec_name=line[0]
            rec_type=line[1]
            res=line[2]
            if len(line)==4:
                value=line[3]
            else:
                value=[]
                for j in range (3, len(line)):
                    value.append(line[j])
            if rec_name=="rec_name":
                continue
            if rec_type=="csvrecorder":
                csvfile=res
                get_csvrecorder_varaiables(csvfile, varaiables_records, start_date, end_date, timeStep, nodes_vars)
                continue
            elif rec_type =="tablesrecorder":
                h5file = res
                get_tablesrecorder_varaiables(h5file, varaiables_records, network,  start_date, end_date, timeStep, nodes_vars)
                continue
            var = resource_varaiable(nodes_vars[res], res, value, start_date, end_date, timeStep)
            varaiables_records.append(var)
    return varaiables_records


def get_csvrecorder_varaiables(csvfile, varaiables_records, start_date, end_date, timeStep, nodes_vars):
    with open(csvfile, 'r') as res:
        contents_ = res.read()
    contents=contents_.split('\n')
    header=contents[0].split(',')
    for i in range(1, len(header)):
        res_name=header[i]
        values=[]
        for j in range(1, len(contents)-1):
            line=contents[j].split(',')
            values.append(line[i])
        if res_name in nodes_vars:
            var = resource_varaiable(nodes_vars[res_name], res_name, values, start_date, end_date, timeStep)
            varaiables_records.append(var)

def get_tablesrecorder_varaiables(h5file, varaiables_records, network, start_date, end_date, timeStep,nodes_vars):
    store=get_h5DF_store(h5file)
    for node in network.nodes:
        values=get_node_attr_values(store, node.name)
        if values !=None:
            var = resource_varaiable(nodes_vars[node.name], node.name, values, start_date, end_date, timeStep)
            varaiables_records.append(var)
        else:
            continue

def import_vars(network, varaiables_records, attrlist):
    attrs = dict()
    resourcescenarios_ids=get_resourcescenarios_ids(network.scenarios[0].resourcescenarios)
    for attr in attrlist:
        attrs.update({attr.id: attr.name})
    attributes_ids = {}
    for attr in attrlist:
        attributes_ids[attr.id] = attr
    res_scenario = network.scenarios[0].resourcescenarios
    for varaiable_record in varaiables_records:
        for node in network.nodes:
            if node.name.lower().strip() == varaiable_record.res.lower().strip():
                for attr_ in node.attributes:
                    if attr_.attr_is_var == 'Y':
                        attr = attrs[attr_.attr_id]
                        if attr== varaiable_record.rec_name:
                            if attr_.id not in resourcescenarios_ids:
                                metadata = {}
                                #continue
                            else:
                                res = resourcescenarios_ids[attr_.id]
                                metadata = json.loads(res.value.metadata)
                            #else:
                            #    metadata={}
                            dataset = dict(name='Pywr import - ' + varaiable_record.rec_name, )
                            dataset['unit'] = '-'
                            dataset['type'] = varaiable_record.type
                            dataset['metadata'] = json.dumps(metadata)
                            dataset['dimension'] = attr_.resourcescenario.value.dimension
                            dataset['value'] = json.dumps({'0': varaiable_record.values})
                            res_scen = dict(resource_attr_id=attr_.id,
                                            attr_id=attr_.attr_id,
                                            value=dataset)
                            res_scenario.append(res_scen)
        network.scenarios[0].resourcescenarios = res_scenario

def save(scenario, connection):
    connection.call('update_scenario', {'scen': scenario})

def get_nodes_variables(network):
    nodes_vars_types = {'output': 'received_water', 'storage': 'storage', 'link': 'flow', 'river': 'flow','reservoir':'storage', 'input': 'mean_flow','catchment':'seasonal_fdc'}
    nodes_vars={}
    for node in network.nodes:
        if node.types==None or len(node.types)==0:
            continue
        type_=(node.types[0]['name']).lower()
        if type_=='aggregatedaode':
            continue
        nodes_vars[node.name]=nodes_vars_types[type_]
    return nodes_vars


def run_pywr_model(file_name, python3_path, args=None):
    cmd=os.path.join(python3_path, "python")
    proc = subprocess.Popen([cmd, 'PywrRunner.py', file_name])
    proc.wait()

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
    text ="Done"
    errors  = []
    steps=12
    try:
        write_progress(1, steps)
        parser = commandline_parser()
        args = parser.parse_args()
        write_progress(2, steps)
        outputfile=check_args(args)
        pywrexporter = PywrExporter(args)
        write_progress(3, steps)
        pywrwriter(pywrexporter.net, pywrexporter.attrlist, outputfile, steps)
        write_progress(7, steps)
        nodes_vars=get_nodes_variables(pywrexporter.net)
        start_time = datetime.now().replace(microsecond=0)
        write_progress(8, steps)

        run_pywr_model(outputfile, args.python3_path)
        write_progress(9, steps)

        results_file = outputfile + ".csv"
        write_progress(10, steps)

        check_output_file(results_file, start_time)
        varaiables_records=import_results(results_file, pywrexporter.net, nodes_vars)
        write_progress(11, steps)

        import_vars(pywrexporter.net, varaiables_records, pywrexporter.attrlist)
        save(pywrexporter.net.scenarios[0], pywrexporter.connection)
        text = PluginLib.create_xml_response('Pywr Auto', (args.network_id), [args.scenario_id],
                                             message="Model run was successful.",
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
    write_progress(12, steps)
    print text



