import sys
import os
import time
import json
from datetime import datetime
from dateutil import parser as prs
from dateutil.relativedelta import relativedelta

import subprocess
from string import ascii_lowercase

from HydraLib.PluginLib import JSONPlugin
from HydraLib.HydraException import HydraPluginError
from HydraLib.hydra_dateutil import reindex_timeseries
from HydraLib import PluginLib

from HydraLib.PluginLib import write_progress, write_output

pythondir = os.path.dirname(os.path.realpath(__file__))
exportpath=os.path.join(pythondir, '..', 'Exporter')
api_path = os.path.realpath(exportpath)
if api_path not in sys.path:
    sys.path.insert(0, api_path)


from PywrJsonWriter import pywrwriter

from PywrJsonWriter import get_resourcescenarios_ids

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

    try:
        output = os.path.dirname(args.output)
        args.output="c:\\temp\\test.json"
        if args.output == '':
            output = '.'

        if  os.path.exists(args.output)==False:
            raise HydraPluginError('Output file directory '+
                                   os.path.dirname(args.output)+
                                   'does not exist')
    except:
        output ="network_"+args.network_id+".json"
    return output


class varaiable_record(object):
    def __init__(self, rec_name, res, value, start_date, end_date, time_step):
        self.values = {}
        start_date=prs. parse(start_date)
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

def export_pywr_network(pywrexporter, args, outputfile, steps):

    pywrwriter(pywrexporter.net, pywrexporter.attrlist, outputfile, steps)
    return "Data export was successful."


def check_output_file(results_file, start_time):
    if os.path.isfile(results_file) == False:
        raise HydraPluginError('No Output file is found')
    dt = prs.parse(time.ctime(os.path.getmtime(results_file)))

    delta = (dt - start_time).total_seconds()
    if delta >= 0:
        pass
    else:
        raise HydraPluginError('No updated Output file is found')

def run_pywr_model(file_name):
    cmd = "PywrRunner.bat " + file_name
    proc = subprocess.Popen(cmd)
    proc.wait()

def import_results(results_file):
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
            res=line[1]
            if len(line)==3:
                value=line[2]
            else:
                value=[]
                for j in range (2, len(line)):
                    value.append(line[j])
            var=varaiable_record(rec_name, res, value, start_date, end_date, timeStep)
            varaiables_records.append(var)
    return varaiables_records


def import_vars(network, varaiables_records, attrlist):
    attrs = dict()
    nodes_recodres={}
    resourcescenarios_ids=get_resourcescenarios_ids(network.scenarios[0].resourcescenarios)

    for attr in attrlist:
        attrs.update({attr.id: attr.name})
    attributes_ids = {}
    for attr in attrlist:
        attributes_ids[attr.id] = attr
    res_scenario = network.scenarios[0].resourcescenarios
    nodes = dict()
    metadata = {}
    for varaiable_record in varaiables_records:
        for node in network.nodes:
            if node.name.lower().strip() == varaiable_record.res.lower().strip():
            #nodes.update({node.id: node.name})
                for attr_ in node.attributes:
                    if attr_.attr_is_var == 'Y':
                        attr = attrs[attr_.attr_id]
                        if attr== varaiable_record.rec_name:
                            res = resourcescenarios_ids[attr_.id]
                            metadata = json.loads(res.value.metadata)
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



def save(network, connection):
    connection.call('update_scenario', {'scen': network.scenarios[0]})

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

        export_pywr_network(pywrexporter, args, outputfile, steps)
        write_progress(7, steps)

        start_time = datetime.now().replace(microsecond=0)
        write_progress(8, steps)

        run_pywr_model(outputfile)
        write_progress(9, steps)

        results_file = outputfile + ".csv"
        write_progress(10, steps)


        check_output_file(results_file, start_time)
        varaiables_records=import_results(results_file)
        write_progress(11, steps)

        import_vars(pywrexporter.net, varaiables_records, pywrexporter.attrlist)
        save(pywrexporter.net, pywrexporter.connection)
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



