import datetime
import json
import os
import random
from collections import namedtuple
import jsonpickle
from dateutil.parser import parse as prs
from Lib.data_files_reader import read_data_file_column, read_tablesarray, read_data_file, read_timeseries,get_value_from_file, read_timeseries_for_scenarios
from datetime import datetime
from datetime import timedelta
from HydraLib.HydraException import HydraPluginError
from Lib.utilities import get_dict, is_in_dict

recourseAttributea =[]
ref_parameters={}
has_tablerecorder=False
timeseries=None
json_file__folder=[]
tables={}
scenarios={}

class Counter(object):
    def __init__(self):
        self.id=-1

class Project (object):
    '''
    Class contains the essential Hydra project elements
    '''
    def __init__(self):
        self.name="Pywer exported project at "+str(datetime.now())
        self.status = 'A'
        self.description='Create by Pywr exporter'
        self.id=-1


class Value (object):
    '''
    Attribute value
    '''
    def __init__(self, value):
        self.value=value
        self.dimen='Dimensionless'
        self.unit='-'

class Attribute (object):
    def __init__(self,name):
        self.name=name
        self.status='A'
        self.description='-'
        self.dimen='dimensionless'
        self.id=-1

class ResourceAttr (object):
    def __init__(self, id, attr_id, type):
        self.id=id
        self.attr_id=attr_id
        if (type.lower() == 'output'):
            self.attr_is_var = 'Y'
        else:
            self.attr_is_var = 'N'

class RecourseAttribute (object):
    def __init__(self,  ref_key, resource_attr_id, attr_id, value_, dimen):
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.source=ref_key
        self.value=value_

class Node(object):
    def __init__(self, node_, counter, nodes_attributes, recorders, nodes_vars_types):
        global  has_tablerecorder
        is_flow=True
        attributes_ = {}
        self.attributes = []
        ras = []
        self.x=None
        self.y=None
        i=0
        self.id = counter.id
        self.status = "A"
        self.name = node_['name']
        self.type = node_['type']

        for recorder in recorders:
            dict=get_dict(recorder)
            #print "dict===>>", dict
            if('node' in dict.keys()):
                del dict["node"]
            del dict['name']
            if recorder.name=='flow':
                is_flow=False
            get_variable_attribute_type_and_value('NULL', recorder.name, counter, attributes_, 'output', dict, self.attributes)
        self.description = ""
        if hasattr(node_, "position"):
            if hasattr(node_.position, "position"):
                if(node_.position.schematic != None and len (node_.position.schematic)==2):
                    self.x=str(node_.position.schematic[0])
                    self.y=str(node_.position.schematic[1])

            if hasattr(node_.position, "geographic"):
                if (node_.position.geographic != None and len(node_.position.geographic) == 2):
                    #counter.id = counter.id - 1
                    #attributes_['geographic'] = \
                    get_attribute_type_and_value(node_.position.geographic, 'geographic', counter, attributes_, self.attributes)
                    #self.attributes.append(ResourceAttr(counter.id, 'geographic', self.type))
                    #recourseAttributea.append(RecourseAttribute('NODE', counter.id, 'geographic', attributes_['geographic'], 'Dimensionless'))

        if(self.x == None):
            self.x = str(random.randint(0,99))
        if(self.y==None):
            self.y = str(random.randint(0,99))


        keys_=sorted(node_.keys())

        for i in range(0, len(keys_)):#.__dict__.keys())):
            k=keys_[i]
            if(k.lower() != 'name' and k.lower() !='type' and k.lower() !='position'):
                val=node_[k]
                if k=='flow':
                    is_flow=False

                get_attribute_type_and_value(val, k, counter, attributes_, 'input', self.attributes)

        get_attribute_type_and_value(node_['type'], 'node_type', counter, attributes_, 'input', self.attributes)
        nodes_attributes [self.name]= attributes_

class Link(object):
    '''
    Hydra link from pywr edge
    '''
    def __init__(self, edge_, nodes_ids, counter, links_attributes):
        self.node_1_id = nodes_ids[edge_[0]]
        self.node_2_id = nodes_ids[edge_[1]]
        self.name = edge_[0] + '_' + edge_[1]
        self.description=""
        self.attributes = []
        self.type='edge'
        if(len(edge_)==4):
            slot_from=edge_[2]
            slot_to=edge_[3]
        else:
            slot_from = None
            slot_to = None
        self.id = counter.id
        self.status = "A"
        self.attributes=[]
        attributes_ = {}
        counter.id = counter.id - 1
        attributes_['slot_from']=AttributeData('descriptor', str(slot_from), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'slot_from','Input'))
        recourseAttributea.append(RecourseAttribute('LINK', counter.id, 'slot_from', attributes_['slot_from'], 'Dimensionless'))
        counter.id = counter.id - 1
        attributes_['slot_to']=AttributeData('descriptor', str(slot_to), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'slot_to', 'Input'))
        recourseAttributea.append(RecourseAttribute('LINK', counter.id, 'slot_to', attributes_['slot_to'], 'Dimensionless'))
        links_attributes[self.name]=attributes_

class Network (object):
    '''
     Create Hydra network
    '''
    def __init__(self, name, solver_name, project_id, counter, domains, recorders, network_attributes, timestepper,  metadata=None):
        '''
        :param name: network name
        :param solver_name: pywr solver name, it will be added as network attribute
        :param project_id: Hydra project id
        :param counter: object contains id which is used to estimate attribute and scenario attributes ids
        :param domains: doamin section inside pywr, it will be saved as network attributes
        :param recorders: conatins pywr recorder section, some of them will be saved as network attributes
        :param network_attributes: other network attributes
        :param timestepper: pywr timestepper
        :param metadata: pywr metadata
        '''
        #print "recorders:=>>", get_dict(recorders)
        global has_tablerecorder
        global scenarios
        attributes_={}
        self.project_id=project_id
        self.type='PywrNetwork'
        author=None
        minimum_version=None
        if(metadata !=None):
            if ("title" in metadata):
                self.name=metadata['title']
            else:
                self.name = name + '_' + str(datetime.now())
            if ( "description" in metadata):
                self.description =metadata['description']
            if "author" in metadata:
                author=metadata['author']
            if ("minimum_version" in metadata):
                minimum_version = metadata['minimum_version']
        else:
            self.name = name + '_' + str(datetime.now())
            self.description = "Pywr network"

        self.id=-1
        self.attributes = []
        self.nodes=[]
        self.links=[]
        self.scenarios=[Scenario()]

        if author is not None:
            counter.id = counter.id - 1
            attributes_['author'] = AttributeData('descriptor', author, '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'author', 'Input'))
            recourseAttributea.append(RecourseAttribute('NETWORK', counter.id, 'author', attributes_['author'], 'Dimensionless'))

        if minimum_version is not None:
            counter.id = counter.id - 1
            attributes_['minimum_version'] = AttributeData('descriptor', minimum_version, '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'minimum_version', 'Input'))
            recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'minimum_version', attributes_['minimum_version'], 'Dimensionless'))

        if(solver_name != None):
            counter.id = counter.id - 1
            attributes_['solver'] = AttributeData('descriptor', solver_name, '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'solver', 'Input'))
            recourseAttributea.append( RecourseAttribute('NETWORK', counter.id, 'solver', attributes_['solver'], 'Dimensionless'))

        if(timestepper!=None):
            counter.id = counter.id - 1
            attributes_['timestep'] = AttributeData('descriptor', str(timestepper['timestep']), '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'timestep', 'Input'))
            recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'timestep', attributes_['timestep'], 'Dimensionless'))

            counter.id = counter.id - 1
            attributes_['start_time'] = AttributeData('descriptor', str(timestepper['start']), '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'start_time', 'Input'))
            recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'start_time', attributes_['start_time'], 'Dimensionless'))

            counter.id = counter.id - 1
            attributes_['end_time'] = AttributeData('descriptor', str(timestepper['end']), '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'end_time', 'Input'))
            recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'end_time', attributes_['end_time'], 'Dimensionless'))

            self.scenarios[0].add_times(str(timestepper['start']),str(timestepper['end']), str(timestepper['timestep'] ))

        domain_list=[]
        for domain in domains:
            domain_list.append([domain.name, domain.color])

        counter.id = counter.id - 1
        attributes_['domains'] = AttributeData('array', json.dumps(domain_list), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'domains', 'Input'))
        recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'domains', attributes_['domains'], 'Dimensionless'))

        recorders_list=[]
        metadata_={}
        for scenario in scenarios:
            metadata = {}
            name=scenarios[scenario]['name']
            metadata['type']='scenario'
            for key in scenarios[scenario]:
                #set metadata value as a string, otherwise, the server will throws
                #StatementError, type error
                metadata[key] = str(scenarios[scenario][key])
            #print "global scenarios", scenario, scenarios[scenario]
            #print metadata
            counter.id = counter.id - 1
            attributes_[name] = AttributeData("descriptor", name, '-', 'Dimensionless', metadata)
            #type, value, unit, dimen,
            self.attributes.append(ResourceAttr(counter.id, name, 'Input'))
            recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, name, attributes_[name], 'Dimensionless'))

        for recorder in recorders:
            #print "1. from network ===>>>", get_dict(recorder)
            if (recorder.name.lower().strip()=="database" or recorder.type=='CSVRecorder') or recorder.type=='polynomial1dcost' and not hasattr(recorder, "node"):
                #print "2. from network ===>>>", get_dict(recorder)
                dic=get_dict(recorder)
                metadata={}
                for key in dic:
                    if key!="name":
                        #if key =='coefficients':
                        if type(dic[key]) is list:
                            metadata[key]=json.dumps(dic[key])
                        elif type(dic[key]) is float :
                            metadata[key] = str(dic[key])
                        else:
                            metadata[key] = dic[key]
                counter.id = counter.id - 1
                if 'name' in dic:
                    p_name=dic['name']
                else:
                    p_name='database'
                metadata['is_recorder']='True'
                attributes_[p_name] = AttributeData("descriptor", p_name, '-', 'Dimensionless', metadata)
                self.attributes.append(ResourceAttr(counter.id, p_name, 'Input'))
                recourseAttributea.append(RecourseAttribute('NETWORK', counter.id, p_name, attributes_[p_name], 'Dimensionless'))
                has_tablerecorder=True
        network_attributes[self.name] = attributes_

class Scenario(object):
    '''
    Hydra scenario
    '''
    def __init__(self):
        self.description="Created by PywrApp"
        self.name="scenarion_" + str(datetime.now())
        self.resourcescenarios=[]
    def add_times(self, start_time, end_time, time_step):
        self.start_time = start_time
        self.end_time = end_time
        self.time_step = time_step

class Resourcescenario(object):
    def __init__(self, source, resource_attr_id, attr_id, value):
        self.source=source
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.value=value

class AttributeData (object):
    def __init__(self,  type, value, unit, dimen, metadata=None):
        self.hidden='N'
        self.type=type
        self.name = 'pywerApp'
        self.unit=unit
        self.dimension=dimen
        if(metadata is None):
            self.metadata='{}'
        else:
            self.metadata =json.dumps(metadata)
        self.value=value

class Recorder(object):
    def __init__(self, name, record_):
        global ref_parameters
        self.name=name
        if "type" in record_.keys():
            self.type = record_['type']
        if "recorders" in record_.keys():
            self.recorders=record_['recorders']
            self.agg_func=record_['agg_func']
        if("node" in record_.keys()):
            self.node=record_['node']
        if "timesteps" in record_.keys():
            self.timesteps=record_['timesteps']
        if  "comment" in record_.keys():
            self.comment = record_['comment']
        if "csvfile" in record_.keys():
            self.csvfile = record_['csvfile']
        if "url" in record_.keys():
            self.url = record_['url']
        if "url" in record_.keys():
            self.url = record_['url']
        if "url" in record_.keys():
            self.url = record_['url']
        if "months" in record_.keys():
            self.months = record_['months']
        if "percentiles" in record_.keys():
            self.percentiles = record_['percentiles']
        if 'agg_func'in  record_.keys():
            self.agg_func=record_['agg_func']
        if 'is_constraint' in record_.keys():
            self.is_constraint=bool(record_['is_constraint'])
        if 'is_objective' in record_.keys():
            self.is_objective=record_['is_objective']
        if 'coefficients' in record_.keys():
            self.coefficients=record_['coefficients']
        if 'epsilon' in record_.keys():
            self.epsilon=record_['epsilon']
        if'parameter' in record_.keys():
            self.parameter=record_['parameter']
        #print "results", get_dict(self)

class Domain (object):
    def __init__(self, domain_):
        self.name=domain_.name
        self.color=domain_.color

def get_timeseriesdates(timestepper):
    '''
    return timeseries from pywr timestepper
    '''
    start=prs(timestepper['start'])
    end=prs(timestepper['end'])
    timestep=(timestepper['timestep'])
    timeseries=[]
    timeseries=[]
    while (end>=start):
        timeseries.append(start)
        start = start + timedelta(days=timestep)
    return timeseries


def read_seasonall(values_):
    from datetime import date
    values = {}
    year = 9999
    month = 1
    day = 1
    #check if the data is saved on file
    if hasattr(values_, 'url'):
        url=values_.url
        ss = os.path.dirname(url)
        if (ss == ''):
            url = os.path.join(json_file__folder[0], url)
        if hasattr(values_, 'column'):
            content, date_index, value_index = read_data_file(url, values_.column)
        else:
            content, date_index, value_index = read_data_file(url)
        for i in range(1, len(content)):
            lin = content[i].split(',')
            ss=int (lin[date_index])
            if (values_.type.lower == 'monthlyprofile'):
                month = ss
                dat = datetime.datetime(year, ss, day)
            else:
                # account for 356 days (leap year) which Hydra 9999 seasonal data does not support
                if(ss>365):
                    continue
                dat=date.fromordinal(date(year, month, day).toordinal() + ss - 1)

            values[str(dat)] = lin[value_index]
    else:
        keys_=values_.keys()
        for v in values_['values']:
            dat=datetime(year, month, day)
            values[str(dat)]=v
            if(values_['type'].lower=='monthlyprofile'):
                month+=1
            else:
                day+=1
    return json.dumps({'0': values})

def get_variable_attribute_type_and_value(value_, name, counter, attributes_, _type, metadata_, res_attributes=None):
    value = value_
    type = 'descriptor'
    metadata={}
    for key in metadata_.keys():
        metadata[key]=str(metadata_[key])

    counter.id = counter.id - 1
    attributes_[name] = AttributeData(type, value, '-', 'Dimensionless', metadata)
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_attribute_type_and_value(value_, name, counter, attributes_, _type, res_attributes=None):
    '''
    Get the attributes types and values in form combatable with hydra
    some pywr attributes are not recognized by hydra as they have more than one attributes
    so, it will be disassemble them to single attributes and set some metadata to define them as pywr attributes
    this ones will be assemebled again when creating Pywr Json string in PywrExporter
    The user needs to be aware about diffrent pywr parameters types, you can get more information from:
    https://pywr.github.io/pywr-docs/tutorial.html

    :param value_: attribute value
    :param name: attribute name
    :param counter: object to set ids
    :param attributes_: current attributes list
    :param _type: attribute type (input or output, i.e. variable)
    :param res_attributes: resources  attributes list
    :return:
    '''
    metadata = {}
    metadata['single'] = 'yes'
    global scenarios
    global ref_parameters
    if type(value_) is dict:
        keys_=value_.keys()
    else:
        keys_={}
    try:
        if "attr_type" in keys_ and value_['type']=='constant':
            metadata['attr_type'] = value_.type
            value = str(float(value_.value))
        else:
            float(value_)
            value = str(value_)
        attr_type = 'scalar'
        if  "attr_type" in keys_:
            metadata['attr_type']=value_['type']
    except:
        if isinstance(value_, list):
            attr_type = "array"
            value = json.dumps(value_)
        elif isinstance(value_, basestring):
            attr=is_in_dict(value_, ref_parameters)
            if attr != None:
                #print "FOUND IT", value_
                attr_type = attr.type
                value = attr.value
                metadata=json.loads(attr.metadata)
                metadata['ref_name']=value_
                if('type' in metadata.keys()):
                    if(metadata['type']=='aggregated'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
                    elif(metadata['type']=='indexedarray'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
                        if ('index_parameter' in metadata.keys()):
                            index_parameter = metadata['index_parameter']
                            get_attribute_type_and_value(index_parameter, index_parameter, counter, attributes_, _type, res_attributes)
                    elif (metadata['type'] == 'controlcurveindex'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
            else:
                value=value_
                attr_type='descriptor'
        elif "type" in keys_:
            metadata['type'] = value_['type'].lower()
            if value_['type'].lower() =='constant':
                if 'value' in value_:
                    value = str(value_['value'])
                elif 'values' in value_:
                    value = str(value_['values'])
                elif 'url' in value_:
                    url = value_['url']
                    column = value_['column']
                    index_col = value_['index_col']
                    index = value_['index']
                    #value = get_value_from_file(value_, json_file__folder)
                    value = get_value_from_file(url, column, index_col,index, json_file__folder)
                    if value==None:
                        raise HydraPluginError("No value is found for: "+name)
                elif 'table' in value_:
                    url=tables[value_['table']]['url']
                    column=value_['column']
                    index=value_['index']
                    index_col=tables[value_['table']]['index_col']
                    value = get_value_from_file(url, column, index_col, index, json_file__folder)
                    if value==None:
                        raise HydraPluginError("No value is found for: "+name)
                attr_type = 'scalar'
            elif value_['type'].lower() == "arrayindexed" or value_['type'].lower() == "dataframe" or value_['type'].lower() == "tablesarray":
                metadata['type'] = value_['type']
                if value_['type'].lower()=='tablesarray':
                    metadata['where']=value_['where']
                    metadata['node'] = value_['node']
                    value=read_tablesarray(value_['url'], value_['where'] ,value_['node'], json_file__folder)
                    attr_type = 'array'
                    if len(scenarios) > 0 and 'scenario' in value_:
                        if 'scenario' in value_.keys():
                            #Will change the tyoe to dataframe when export the json from hydra
                            #with scenario needs to be changed
                            metadata["scenario"] = value_['scenario']

                else:
                    attr_type = 'timeseries'
                    sheetname=None
                    if 'sheetname' in keys_:
                        sheetname=value_['sheetname']
                    if  "column" in value_:
                       url=os.path.join(json_file__folder[0], value_['url'])
                       value = read_timeseries(url,sheetname,  name, json_file__folder, value_['column'])
                       metadata['column']=value_['column']
                    else:
                        if len(scenarios)>0 and  'scenario' in value_:
                            if 'checksum' in value_.keys():
                                '''
                                ToDo implement checksum for files
                                '''
                                pass

                            if 'scenario' in value_.keys():
                                metadata["scenario"] = value_['scenario']
                            value =read_timeseries_for_scenarios(value_['url'], name, scenarios[value_['scenario']], sheetname, json_file__folder)
                        else:
                            value = read_timeseries(value_['url'], name, sheetname, json_file__folder)
                if  "parse_dates" in keys_:
                    metadata['parse_dates'] = str(value_['parse_dates'])
                if "index_col" in keys_:
                    metadata['index_col'] = str(value_['index_col'])
                if "dayfirst" in keys_:
                    metadata['dayfirst'] = str(value_['dayfirst'])

            elif value_['type'].lower()== "monthlyprofile" or value_['type'].lower()== "dailyprofile" :
                attr_type = 'timeseries'
                metadata['type']=value_['type']
                value = read_seasonall(value_)
                if "column" in keys_:
                    metadata['column'] = value_['column']
                if "parse_dates" in keys_:
                    metadata['parse_dates'] = str(value_['parse_dates'])
            elif value_['type'].lower() == 'aggregated':
                get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'indexedarray':
                get_indexedarray_attributes(value_, name, counter, attributes_, _type,  res_attributes)
                return
            elif value_['type'].lower() == 'controlcurveindex':
                get_controlcurveindex_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() =='monthlyprofilecontrolcurve':
                get_monthlyprofilecontrolcurve_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'controlcurveinterpolated':
                get_controlcurveinterpolated_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'recorderthreshold':
                get_recorderthreshold(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type']=='constantscenario':
                get_constantscenario(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type']=='annualharmonicseries':
                get_Annualharmonicseries(value_, name, counter, attributes_, _type, res_attributes)
                return

    if "comment" in keys_:
        metadata['comment']=value_['comment']
    counter.id = counter.id - 1
    if type(value_) is dict:
        add_metdata(value_, metadata)

    attributes_[name] = AttributeData(attr_type, value, '-', 'Dimensionless', metadata)
    if(res_attributes!=None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name , attributes_[name],'Dimensionless'))

def add_metdata(value_, metadata):
    for key in value_:
        if is_in_dict(key, metadata) ==None:
            #print value_[key]
            metadata[key]=str(value_[key])

'''
    The flowing functions are used to get specific pywr parameters in a hydra resource attributes
    functions names indicates the pywr parameter  
    Metadata is used to define description of the attributes which are used by pywr
'''
def get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes=None):
    metadata={}
    type = 'array'
    value = json.dumps(value_['parameters'])
    metadata['single'] = 'no'
    metadata['agg_func'] = value_['agg_func']
    metadata['type'] = 'aggregated'
    if  "comment" in value_:
        metadata['comment'] = value_['comment']
    counter.id = counter.id - 1
    attr= AttributeData(type, value, '-', 'Dimensionless', metadata)
    attributes_[name] =attr
    if(res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))
    get_aggregated (json.loads(attributes_[name].value), counter, attributes_,_type,  res_attributes)

def get_aggregated(paras, counter, attributes_, _type, res_attributes):
    for para in paras:
        get_attribute_type_and_value(para, para, counter, attributes_, _type, res_attributes)

def get_indexedarray_attributes(value_, name, counter, attributes_, _type, res_attributes=None):
    metadata={}
    type = 'array'
    metadata['single'] = 'no'
    metadata['type'] = 'indexedarray'
    if "comment" in value_:
        metadata['comment'] = value_['comment']
    index_list = get_indexedarray(value_['params'], name, counter, attributes_, _type, res_attributes=None)
    value = json.dumps(index_list)

    if "index_parameter" in value_:
        index_parameter=value_['index_parameter']
        metadata['index_parameter']=index_parameter
    attr = AttributeData(type, value, '-', 'Dimensionless', metadata)
    counter.id = counter.id - 1
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_indexedarray(params, name, counter, attributes_, _type, res_attributes=None):
    index_list=[]
    ind=1
    for item in params:
        ind += 1
        name_ = name + "_" + str(ind)
        get_attribute_type_and_value(item, name_, counter, attributes_, _type, res_attributes)
        index_list.append(name_)
    return index_list

def get_controlcurveindex_attributes(value_, name, counter, attributes_, _type, res_attributes):
    metadata = {}
    type='array'
    metadata ['type'] = 'controlcurveindex'
    metadata['single'] = 'no'
    if "storage_node" in value_:
        metadata['storage_node']=value_["storage_node"]
    if "comment" in value_:
        metadata['comment'] = value_['comment']
    value=value_['control_curves']
    for item in value:
        get_attribute_type_and_value(item, item, counter, attributes_, _type, res_attributes)
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    counter.id = counter.id - 1
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_monthlyprofilecontrolcurve_attributes(value_, name, counter, attributes_, _type, res_attributes):
    metadata = {}
    type = 'array'
    metadata['type'] = 'monthlyprofilecontrolcurve'
    metadata['single'] = 'no'
    if  "storage_node" in value_:
        metadata['storage_node'] = value_['storage_node']
    if "comment" in value_:
        metadata['comment'] = value_['comment']
    if "scale" in value_:
        metadata['scale'] = str(value_['scale'])
    values_list=[name+'_profile', name+'_control_curve', name+'_values']

    get_attribute_type_and_value(value_['profile'], name + '_profile', counter, attributes_, _type, res_attributes)
    get_attribute_type_and_value(value_['control_curve'], name + '_control_curve', counter, attributes_, _type, res_attributes)
    get_attribute_type_and_value(value_['values'], name + '_values', counter, attributes_, _type, res_attributes)
    counter.id = counter.id - 1
    attr = AttributeData(type, json.dumps(values_list), '-', 'Dimensionless', metadata)
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_recorderthreshold(value_, name, counter, attributes_, _type, res_attributes):
    type = 'array'
    metadata = {}
    metadata['single'] = 'no'
    metadata['type'] = 'recorderthreshold'
    if "comment" in value_:
        metadata['comment'] = value_['comment']
    metadata['recorder']=value_['recorder']
    metadata['threshold']=str(value_['threshold'])
    metadata['predicate']=value_['predicate']
    value=value_['values']
    counter.id = counter.id - 1
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_controlcurveinterpolated_attributes(value_, name, counter, attributes_, _type, res_attributes):
    type = 'array'
    metadata = {}
    metadata['single'] = 'no'
    if "comment" in value_:
        metadata['comment'] = value_['comment']

    if "storage_node" in value_:
        metadata['storage_node'] = value_['storage_node']

    control_curve=value_['control_curve']
    get_attribute_type_and_value(control_curve, name + 'control_curve', counter, attributes_, _type, res_attributes)
    counter.id = counter.id - 1
    value= value_['values']

    metadata['control_curve']=name + 'control_curve'

    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)

    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_metadate_value(values, key):
    try:
        return json.dumps(values[key])
    except:
        raise HydraPluginError("No value is found for: "+key)

def get_Annualharmonicseries(value_, name, counter, attributes_, _type, res_attributes):
    type='array'
    metadata = {}
    metadata['single'] = 'yes'
    metadata['type']='annualharmonicseries'
    value = {}
    value['mean']=value_['mean']
    value['amplitudes']=value_['amplitudes']
    value['phases']=value_['phases']
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    attributes_[name] = attr
    counter.id = counter.id - 1
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_constantscenario(value_, name, counter, attributes_, _type, res_attributes):
    type='array'
    metadata = {}
    metadata['single'] = 'yes'
    metadata['type']='constantscenario'
    metadata['scenario']=value_['scenario']
    value = value_['values']
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    attributes_[name] = attr
    counter.id = counter.id - 1
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_new_attributes(attrlist, c_attrlist):
    '''
    Get the new attributes which are used in this model to add them to the Hydra
    :param attrlist: Current Hydra attributes list
    :param c_attrlist: Attributes
    :return: new attributes list
    '''
    vars_attr = ['received_water', 'storage', 'flow', 'mean_flow']
    new_attr=[]
    to_be_added = []
    for var_attr in vars_attr:
        is_in = False
        for attr in attrlist:
            if attr.name.lower() == var_attr:
                is_in = True
                break
        if is_in == False:
            attr = Attribute(var_attr)
            to_be_added.append(attr)

    attrlist += to_be_added

    for attr in attrlist:
        is_in=False
        for eattr in c_attrlist:
            if(attr.name.lower() == eattr.name.lower() and attr.dimen.lower() == eattr.dimen.lower):
                is_in=True
                break
        if is_in==False:
            new_attr.append(get_dict(attr))

    return new_attr

def adjust_links_nodes(nodes_to_links, not_added_links, nodes_ids):
    '''
    #Add from and to nodes to hydra links which is orginally pywr links nodes
    :param nodes_to_links: dict contains pywr link nodes which has been save as hydra link
    :param not_added_links: hydra edhes which are not added to hydra links as they have pywr linked node
    :param nodes_ids: dict contains names as keys and id as values
    '''
    for node_name in nodes_to_links.keys():
        for edge_ in not_added_links:
            if edge_[0]==node_name:
                nodes_to_links[node_name].node_2_id=nodes_ids[edge_[1]]
            elif edge_[1]==node_name:
                nodes_to_links[node_name].node_1_id= nodes_ids[edge_[0]]

def get_pywr_json_from_file(filename):
    json_list = []
    f = open(filename, 'r')
    json_string = ""
    while 1:
        line = f.readline()
        if not line: break
        json_string += line
    f.close()
    main_json = json.loads(json_string)  # , object_hook=lambda d: namedtuple('X', d.keys(), rename=False, verbose=False)(*d.values()))
    json_list.append(main_json)
    if "includes" in main_json:
        for included_file in main_json['ncludes']:
            ss = os.path.dirname(included_file)
            if (ss == ''):
                included_file = os.path.join(json_file__folder[0], included_file)
            included_f = open(included_file, 'r')
            included_json_string = ""
            while 1:
                line = included_f.readline()
                if not line: break
                included_json_string += line
            included_f.close()

            included_x = json.loads(included_json_string, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            json_list.append(included_x)

            if ("nodes" in included_x):
                for node in included_x['nodes']:
                    main_json.nodes.append(node)

            if "edges" in included_x:
                for edge in included_x['edges']:
                    main_json.edges.append(edge)
            # if hasattr(included_x, "parameters"):
            #    for key_ in included_x.parameters:
            #        if hasattr(main_json, "parameters"):
            #           main_json.parameters[key_]=included_x.parameters[key_]

    return main_json, json_list

def import_net (filename, c_attrlist, connector=None):
    '''
    Main function which reads the pywr model JSON  string from a file
    It will extracts the main network elements from it and set them in a form which is recognized by Hydra
    then it will push to hydra database
    :param filename: name of the file  which conatins the Json string
    :param connector: a connector object which is used to communicates with Hydra
    :return: When it runs successfully, i.e. reads the json string, creates Hydra network and its elements, pushed to Hydra
    It will return a Hydra network summary
    '''
    has_tablerecorder = False
    # sometimes json string has include statment which include another file whcih contains the
    # another JSON string, if it is the case it will be added to json_list to added them to the model
    json_file__folder.append(os.path.dirname(filename))
    counter=Counter()
    recorders=[]
    domains=[]
    project_attributes = {}
    nodes_types={}
    global ref_parameters
    links_types={}
    # dict contains the main pywr variables according to the node type
    nodes_vars_types = {'output': 'received_water', 'storage': 'storage', 'link': 'flow', 'reservoir': 'storage', 'input':'mean_flow', 'catchment':'seasonal_fdc'}

    nodes_attributes={}
    links_attributes = {}
    network_attributes={}
    main_json, json_list=get_pywr_json_from_file(filename)
    global timeseries
    if "timestepper" in main_json:
        timeseries=get_timeseriesdates(main_json['timestepper'])

    global scenarios
    if "scenarios" in main_json:
        for scenario in main_json['scenarios']:
            scenarios[scenario['name']]={"name": scenario['name'], "size": scenario['size']}

    project=Project()
    request=jsonpickle.dumps(project).replace("/","_").replace("\"", '\'')

    if ("recorders" in main_json):
        rcds=get_dict(main_json['recorders'])
        for kk in rcds.keys():
            recorders.append(Recorder(kk, rcds[kk]))

    if ("domains" in main_json):
        for domain_ in main_json['domains']:
            domains.append(Domain(domain_))

    if ("tables" in main_json):
        for table in main_json['tables']:
            tables[table]= main_json['tables'][table]

    timestepper=None
    if ("timestepper" in main_json):
        timestepper = main_json['timestepper']

    solver_name=None
    if ("solver" in main_json):
        solver_name=main_json['solver']

    if ("metadata" in main_json):
        network = Network('Pywr',solver_name, -1, counter, domains, recorders, network_attributes,
                          timestepper, main_json['metadata'])
    else:
        network = Network('Pywr', solver_name, -1, counter, domains, recorders, network_attributes,
                          timestepper)
    #add new new network attributes to project attributes
    for attr_name in network_attributes[network.name].keys():
        if (attr_name in project_attributes):
            pass
        else:
            attr = Attribute(attr_name)
            project_attributes[attr_name] = attr

    counter.id=counter.id-1
    nodes_ids={}
    #dict contains nodes names as keys and nodes ids as values
    i = 0
    for json_ in json_list:
        #print "Parameters section is found...."
        if ("parameters" in json_):
            for i in range(0, len(json_['parameters'].keys())):
                k = json_['parameters'].keys()[i]
                counter.id = counter.id - 1
                get_attribute_type_and_value(json_['parameters'].values()[i], k, counter, ref_parameters, 'default')
                #print "ref_parameters==>>",get_dict(ref_parameters)
    nodes_to_links={}
    # dict to saves pywr links which are saved as Hydra nodes,
    # It will be used later to check the edges section
    not_added_links=[]
    # list used to store the edges which is not added to hydra links
    # because they contains from and to nodes for links nodes
    # It will be used to adjust the hydra links
    for node_ in main_json['nodes']:
        node_records=[]
        for rec in recorders:
             if hasattr (rec, "node"):
                if rec.node==node_['name']:
                     node_records.append(rec)
        node = Node(node_, counter, nodes_attributes, node_records, nodes_vars_types)
        #check node type and if it is a link it will be added to the links instaed of nodes list
        #to do applay the same for rivers
        '''
        if node.type=='link':
            if node.type not in links_types:
                links_types[node.type] = [node.name]
            else:
                links_types[node.type].append(node.name)
            network.links.append(node)
            nodes_to_links[node.name]=node
       
        else:
         '''
        nodes_ids[node_['name']] = node.id
        if node.type in nodes_types.keys():
            nodes_types[node.type].append(node.name)
        else:
            nodes_types[node.type] = [node.name]
        network.nodes.append(node)
        counter.id = counter.id - 1
        for attr_name in nodes_attributes[node.name].keys():
            if( attr_name in project_attributes):
                pass
            else:
                attr=Attribute(attr_name)
                project_attributes[attr_name]=attr
    ## add pywr edges to hydra network links
    for edge_ in main_json['edges']:
       if edge_[0] in nodes_to_links or edge_[1] in nodes_to_links:
           not_added_links.append(edge_)
           continue
       link=Link(edge_, nodes_ids, counter, links_attributes)
       if link.type not in links_types:
           links_types[link.type]=[link.name]
       else:
           links_types[link.type].append(link.name)
       counter.id = counter.id - 1
       network.links.append(link)
       for attr_name in links_attributes[link.name].keys():
           if (attr_name not in project_attributes):
               attr = Attribute(attr_name)
               project_attributes[attr_name] = attr
    adjust_links_nodes(nodes_to_links, not_added_links, nodes_ids)
    new_attr = get_new_attributes(project_attributes.values(), c_attrlist)
    if connector!=None:
        attributes = connector.call('add_attributes', {'attrs': new_attr})
    else:
        attributes=c_attrlist
    attrs_names_ids={}
    for tt in attributes:
        attrs_names_ids[tt.name]=tt.id
    for rs in recourseAttributea:
        try:
            rs.attr_id=attrs_names_ids[rs.attr_id]
        except:
            pass#print "Not found-------------->", rs.attr_id
    network.scenarios[0].resourcescenarios=recourseAttributea

    for rs in network.attributes:
        rs.attr_id = attrs_names_ids[rs.attr_id]
    for node in network.nodes:
        for rs in node.attributes:
            rs.attr_id=attrs_names_ids[rs.attr_id]
    for link in network.links:
        for rs in link.attributes:
            rs.attr_id = attrs_names_ids[rs.attr_id]
    #Push the network to Hydra

    return network,nodes_types, links_types

'''
    This function has be token from ImportCSV.py and has been modified to work with the app
    It is used to upload the template to the server and set types to nodes and links
'''
def add_network(network, connector,nodes_types, links_types):
    project=Project()
    proj = connector.call('add_project', {'project': project.__dict__})
    network.project_id=proj.id
    NetworkSummary = connector.call('add_network', {'net': get_dict(network)})
    # Push pywr template to Hydra and set nodes and links types
    set_resource_types(nodes_types, links_types, network.type, NetworkSummary, connector)
    return NetworkSummary


def set_resource_types(nodes_types, links_types, networktype, NetworkSummary, connection):
    template_file='..\PywrNetwork.xml'
    #print("Setting resource types based on %s." % template_file)
    with open(template_file) as f:
        xml_template = f.read()
    template = connection.call('upload_template_xml', {'template_xml':xml_template})
    type_ids = {}
    warnings = []
    for tmpltype in template.get('types', []):
        if tmpltype['name'].lower() == (str(networktype).lower()):
            type_ids.update({tmpltype['name']: tmpltype['id']})
            break

    for type_name in nodes_types:
        for tmpltype in template.get('types', []):
            if tmpltype['name'] == (str(type_name).lower()):
                type_ids.update({tmpltype['name']: tmpltype['id']})
                break

    for type_name in links_types:
        for tmpltype in template.get('types', []):
            if tmpltype['name'] == type_name.lower():
                type_ids.update({tmpltype['name']: tmpltype['id']})
                break
    for tmpltype in template.get('types', []):
        if tmpltype['name'] == networktype.lower():
            type_ids.update({tmpltype['name']: tmpltype['id']})
            break
    args = []

    if networktype == '':
        warnings.append("No network type specified")
    elif type_ids.get(networktype):
        args.append(dict(
            ref_key = 'NETWORK',
            ref_id  = NetworkSummary['id'],
            type_id = type_ids[networktype],
        ))
    if NetworkSummary.get('nodes', []):
        for node in NetworkSummary['nodes']:
            for typename, node_name_list in nodes_types.items():
                type_id=is_in_dict(typename, type_ids)
                if type_id !=None  and node.name in node_name_list:
                    args.append(dict(
                        ref_key = 'NODE',
                        ref_id  = node.id,
                        type_id = type_id,
                    ))

    if NetworkSummary.get('links', []):
        for link in NetworkSummary['links']:
            for typename, link_name_list in links_types.items():
                type_id = is_in_dict(typename, type_ids)
                if type_id !=None and link['name'] in link_name_list:
                    args.append(dict(
                        ref_key = 'LINK',
                        ref_id  = link['id'],
                        type_id = type_id ,
                    ))

    connection.call('assign_types_to_resources', {'resource_types':args})