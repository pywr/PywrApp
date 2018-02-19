'''
(c) 2017 University of Manchester\

plugin_name: RunOptimisationProcess

This app export the Hydranetwork into Pywr json file, runs optimisation process extract the process results and pushes them to the Hydra

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
--polyviz             -pv     Generate csv input file to be used with polyviz.

Example:
=========
       Python RunOptimisationProcess -t 16 -s 16 -p "C:\ProgramData\Anaconda3\envs\pywr" -r "F:\work\Apps\pywr-to-borg\examples\simple_reservoir_system.py"
'''

import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import subprocess
from HydraLib.HydraException import HydraPluginError
from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output
from dateutil import parser as prs
from Lib.data_files_reader import get_h5DF_store, get_node_attr_values
from Exporter.PywrJsonWriter import pywrwriter
from Exporter.PywrJsonWriter import get_resourcescenarios_ids
from PywrAuto.PywrAuto import  run_pywr_model, PywrExporter, check_args
from Lib.utilities import  check_output_file
from ployvis_file_writer import write_polyviz_file
import json
import logging
import argparse as ap
from Lib.utilities import  check_output_file
from PywrAuto.PywrAuto import save

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


    parser.add_argument('-pv', '--polyviz', action='store_true',
                            help="""Generate csv input file to be used with polyviz.""")

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

    parser.add_argument('-r', '--optmisation-runner-code',
                        help='''python code which is used to the run the optmisation process''')

    return parser


def run_opt_process(args, max_evaluations, arche_file, json_model_file):
    python3_path=args.python3_path
    #    process_args = args.optmisation_runner_code+' search --max-evaluations=' + str( max_evaluations) + ' --json-model-file=' +  ' ' + arche_file
    pythondir = os.path.dirname(os.path.realpath(__file__))

    jsm=os.path.join(pythondir, json_model_file)

    #process_args = ['search', ' --max-evaluations='+ str(max_evaluations),' ',arche_file, ' --json-model-file='+ jsm]
    cmd=os.path.join(python3_path, "python")

    cmd =cmd+' '+ args.optmisation_runner_code+' search'+ ' --max-evaluations=' + str(max_evaluations)+ ' '+ arche_file+' --json-model-file=' + jsm

    proc = subprocess.Popen(cmd)#, args.optmisation_runner_code, process_args])
    proc.wait()

def extract_output(arch_file):
    f = open(arch_file, 'r')
    items_string = ""
    while 1:
        line = f.readline()
        if not line: break
        items_string += line
    f.close()
    items = json.loads(items_string)
    objectives={}
    constraints={}
    variables={}
    for i in range(0, len(items)+1):
        id=+i
        print "Reading solution no: ", id
        item=items[0]
        objs = list(item['objectives'])
        objs.sort()
        for obj in objs:
            if i==0:
                values={}
                objectives[obj]=values
            else:
                values=objectives[obj]
            values[i]=(item['objectives'][obj])
        cons = list(item['constraints'])
        cons.sort()
        for con in cons:
            if i==0:
                c_values={}
                constraints[con]=c_values
            else:
                c_values=constraints[con]
            c_values[i]=(item['constraints'][con])

        vars_ = list(item['variables'])
        vars_.sort()
        for var in vars_:
            if i==0:
                v_vaues={}
                variables[var]=v_vaues
            else:
                v_vaues=variables[var]
            if type(item['variables'][var]) is list:
                v_vaues[i]=(item['variables'][var][0])
            else:
                v_vaues[i] = (item['variables'][var])
    return objectives, constraints, variables

def get_vars(pywr_model, vars):
    parameters = pywr_model['parameters']
    nodes = pywr_model['nodes']
    for parameter_name in parameters:
        metatdata={}
        parameter = parameters[parameter_name]
        if 'is_variable' in parameter:
            metatdata['is_variable'] = parameter['is_variable']
            vars[parameter_name]=metatdata
        else:
            continue
        for node in nodes:
            if parameter_name in node.values():
                attr_name = node.keys()[node.values().index(parameter_name)]
                metatdata['name'] = attr_name
                node_name = node['name']
                metatdata['node'] = node_name
                break

def get_parameters_and_nodes_for_results(pywr_model, metatdata, parameter_):
    parameters=pywr_model['parameters']
    nodes=pywr_model['nodes']
    for parameter_name in parameters:
        if parameter_!=parameter_name:
            continue
        parameter=parameters[parameter_name]
        if 'is_variable' in parameter:
            metatdata['is_variable']=parameter['is_variable']
        for node in nodes:
            if parameter_name in node.values():
                attr_name=node.keys()[node.values().index(parameter_name)]
                #metatdata['name'] = attr_name
                node_name=node['name']
                metatdata['node'] = node_name
                break


def get_hydra_attributes(objectives, variables, constraints, pywr_json_string):
    pywr_model=json.loads(pywr_json_string)
    recorders=pywr_model['recorders']
    objs={}
    consts={}
    vars={}

    for rec_name in recorders.keys():
        metatdata={}
        rec=recorders[rec_name]
        attribute_ = rec['type']
        metatdata['name']=attribute_
        if 'is_objective' in rec:
            is_objective=rec['is_objective']
            objs[rec_name]=metatdata
        elif 'is_constraint' in rec:
            is_constraint=rec['is_constraint']
            consts[rec_name]=metatdata
        elif 'is_variable' in rec:
            is_variable=rec['is_variable']
            vars[rec_name]=metatdata
        else:
            continue
        if 'node' in rec:
            node_name=rec['node']
            metatdata['node']=node_name
        else:
            if 'parameter' in rec:
                parameter=rec['parameter']
                #metatdata['parameter']=parameter
                get_parameters_and_nodes_for_results(pywr_model, metatdata, parameter)

    get_vars(pywr_model, vars)
    return  objs,consts, vars

def create_res_scenario():
    pass

def import_vars(network, created_scenario, vars, values, attrlist, attrs, resourcescenarios_ids, attr_rule):
    res_scenario = created_scenario.resourcescenarios
    for varaiable_record in vars:
        metadata_=vars[varaiable_record]
        for node in network.nodes:
            if node.name.lower().strip() == metadata_['node'].lower().strip():
                for attr_ in node.attributes:
                    if attr_.attr_is_var == 'Y'  or attr_rule=='variable':
                        attr = attrs[attr_.attr_id]
                        if attr== metadata_['name']:
                            if attr_.id not in resourcescenarios_ids:
                                metadata = {}
                                #continue
                            else:
                                res = resourcescenarios_ids[attr_.id]
                                metadata = json.loads(res.value.metadata)
                            metadata['attr_rule']=attr_rule
                            dataset = dict(name='Pywr import - ' + varaiable_record)
                            dataset['unit'] = '-'
                            dataset['type'] = 'array'
                            dataset['metadata'] = json.dumps(metadata)
                            dataset['dimension'] = attr_.resourcescenario.value.dimension
                            dataset['value'] = json.dumps({'0': values[varaiable_record]})
                            res_scen = dict(resource_attr_id=attr_.id,
                                            attr_id=attr_.attr_id,
                                            value=dataset)
                            res_scenario.append(res_scen)
        created_scenario.resourcescenarios = res_scenario

def clone_main_scenario(connection, scenario_id):
    #clone_scenario
    clonned_id= (connection.call('clone_scenario', {'scenario_id': scenario_id})).id
    return (connection.call('get_scenario', {'scenario_id': clonned_id}))

def get_modified_scenario(connection, scenario_id):
    return connection.call('get_scenario', {'scenario_id': scenario_id})

if __name__ == '__main__':
    max_evaluations=1000
    script_path = os.path.dirname(os.path.realpath(__file__))
    arche_file=os.path.join(script_path, "arche.json")
    text ="Done"
    errors  = []
    steps=12
    try:
        write_progress(1, steps)
        parser = commandline_parser()
        args = parser.parse_args()
        write_progress(2, steps)
        if args.optmisation_runner_code == None:
            raise HydraPluginError('No optmisation runner code is specified')
        outputfile=check_args(args)
        outputfile=os.path.join(script_path, outputfile)
        pywrexporter = PywrExporter(args)
        write_progress(3, steps)
        pywr_json_string=pywrwriter(pywrexporter.net, pywrexporter.attrlist, outputfile, steps)
        start_time = datetime.now().replace(microsecond=0)
        write_progress(4, steps)
        #run_opt_process(args, max_evaluations, arche_file, outputfile)
        #check_output_file(arche_file, start_time)
        objectives, constraints, variables=extract_output(arche_file)
        objs, consts, vars=get_hydra_attributes(objectives, variables, constraints, pywr_json_string)
        attrs = dict()
        for attr in pywrexporter.attrlist:
            attrs.update({attr.id: attr.name})
        attributes_ids = {}
        for attr in pywrexporter.attrlist:
            attributes_ids[attr.id] = attr

        created_scenario=clone_main_scenario(pywrexporter.connection, int(args.scenario_id))
        resourcescenarios_ids = get_resourcescenarios_ids(created_scenario.resourcescenarios)
        import_vars(pywrexporter.net, created_scenario, objs, objectives, pywrexporter.attrlist, attrs, resourcescenarios_ids, 'objective')
        import_vars(pywrexporter.net, created_scenario, vars, variables, pywrexporter.attrlist, attrs, resourcescenarios_ids, 'variable')
        import_vars(pywrexporter.net, created_scenario, consts, constraints, pywrexporter.attrlist, attrs, resourcescenarios_ids, 'constraint')
        save(created_scenario, pywrexporter.connection)

        #8
        if args.polyviz==True:
            write_polyviz_file(pywrexporter.net, get_modified_scenario(pywrexporter.connection, created_scenario.id), pywrexporter.attrlist)
            #write_polyviz_file(pywrexporter.net, get_modified_scenario(pywrexporter.connection, 8),
                               #pywrexporter.attrlist)

        text = PluginLib.create_xml_response('Run Optimisation Process', (args.network_id), [args.scenario_id],
                                             message="Model run was successful, parteo set is saved in a new scenario which has id: "+str(created_scenario.id),
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



