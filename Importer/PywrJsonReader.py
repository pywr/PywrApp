import os
import json
import jsonpickle
import random
from collections import namedtuple
from dateutil.parser import parse as prs
from datetime import datetime
from datetime import timedelta
import datetime
from HydraLib.HydraException import HydraPluginError
from data_files_reader import read_data_file_column, get_h5DF_store, get_node_attr_values, read_hdf_file_column

recourseAttributea =[]
ref_parameters={}


has_tablerecorder=False
timeseries=None
json_file__folder=[]

class Counter(object):
    def __init__(self):
        self.id=-1

class Project (object):
    def __init__(self):
        self.name="Pywer exported project at "+str(datetime.datetime.now())
        self.status = 'A'
        self.description='Create by Pywr exporter'


class Value (object):
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
    def __init__(self, node_, counter, nodes_attributes, recorders):
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
        print ("====>", type(node_))
        self.name = node_['name']
        self.type = node_['type']
        for recorder in recorders:
            dict=get_dict(recorder)
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
        print ("====>",node_)

        keys_=sorted(node_.keys())
        print "Node keys: ", keys_

        for i in range(0, len(keys_)):#.__dict__.keys())):
            k=keys_[i]
            if(k.lower() != 'name' and k.lower() !='type' and k.lower() !='position'):
                val=node_[k]
                if k=='flow':
                    is_flow=False
                print "___________________--------------------->",  self.name
                print k
                get_attribute_type_and_value(val, k, counter, attributes_, 'input', self.attributes)
        if is_flow == True and has_tablerecorder==True:
            get_variable_attribute_type_and_value('NULL', 'mean_flow', counter, attributes_, 'output', {}, self.attributes)




        get_attribute_type_and_value(node_['type'], 'node_type', counter, attributes_, 'input', self.attributes)
        nodes_attributes [self.name]= attributes_
        print get_dict(attributes_)

class Link(object):
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
    def __init__(self, name, solver_name, project_id, counter, domains, recorders, network_attributes, timestepper,  metadata=None):
        global has_tablerecorder
        attributes_={}
        self.project_id=project_id
        author=None
        minimum_version=None
        if(metadata !=None):
            if ("title" in metadata):
                self.name=metadata['title']
            if ( "description" in metadata):
                self.description =metadata['description']
            if "author" in metadata:
                author=metadata['author']
            if ("minimum_version" in metadata):
                minimum_version = metadata['minimum_version']
        else:
            self.name = name + '_' + str(datetime.datetime.now())
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
        for recorder in recorders:
                if recorder.name.lower().strip()=="database" and not hasattr(recorder, "node"):
                    dic=get_dict(recorder)
                    print "RECODER IS FOUND ...................", recorder.name
                    print dic.keys()
                    metadata={}
                    for key in dic:
                        if key!="name":
                            metadata[key]=dic[key]
                    '''
                    recorders_list.append(recorder.name)
                    for k in dic.keys():
                        if(k !='name'):
                            metadata_[recorder.name+'@'+k]=str(dic[k])
                    '''
                    counter.id = counter.id - 1
                    attributes_['recorder'] = AttributeData("descriptor", "database", '-', 'Dimensionless', metadata)
                    self.attributes.append(ResourceAttr(counter.id, "recorder", 'Input'))
                    recourseAttributea.append(RecourseAttribute('NETWORK', counter.id, 'recorder', attributes_['recorder'], 'Dimensionless'))
                    has_tablerecorder=True
        network_attributes[self.name] = attributes_

class Scenario(object):
    def __init__(self):

        self.description="Created by PywrApp"
        self.name="scenarion_" + str(datetime.datetime.now())
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
        print "Type: ", type
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
        #####################################
        if "url" in record_.keys():
            self.url = record_['url']
        if "url" in record_.keys():
            self.url = record_['url']

'''
class Recorder(object):
    def __init__(self, record_):
        self.type = record_.type
        if hasattr(record_, "recorders"):
            self.recorders=record_.recorders
            self.agg_func=record_.agg_func
        else:
            self.node=record_.node
        if hasattr(record_, "timesteps"):
            self.timesteps=record_.timesteps
        if hasattr(record_, "comment"):
            self.comment = record_.comment
'''
class Domain (object):
    def __init__(self, domain_):
        self.name=domain_.name
        self.color=domain_.color

def get_timeseriesdates(timestepper):
    start=prs(timestepper['start'])
    end=prs(timestepper['end'])
    timestep=(timestepper['timestep'])
    timeseries=[]
    timeseries=[]
    while (end>=start):
        timeseries.append(start)
        start = start + timedelta(days=timestep)
    return timeseries

def read_data_file(url, column=None):
    with open(os.path.join(url)) as f:
        content = f.read().splitlines()
        head = content[0].split(',')
    if column is not None:
        for i in range(0, len(head)):
            if (head[i].strip().lower() == 'date' or head[i].strip().lower() == 'index'):
                date_index = i
            elif (head[i].strip().lower() == column.lower()):
                value_index = i
    else:
        if (head[0].lower() == 'date' or head[0].lower() == 'Index'):
            date_index = 0
            value_index = 1
        else:
            date_index = 1
            value_index = 0
    return content, date_index, value_index


def read_tablesarray(url, root, data_node):
    ss = os.path.dirname(url)
    if (ss == ''):
        url = os.path.join(json_file__folder[0], url)
    store=get_h5DF_store(url)
    values_= get_node_attr_values(store, data_node, root)
    return json.dumps( values_.tolist())

def read_timeseries(url, name,  sheetname, column=None):
    ss= os.path.dirname(url)
    if(ss == ''):
        url=os.path.join(json_file__folder[0], url)
    values_ = read_data_file_column(url, name, column, sheetname)
    '''
    values={}
    date_format = "%d/%m/%Y"

    for date_ in values_:
        date__ = get_datetime(date_)# datetime.datetime.strptime(date_, date_format)
        values[str(date__)] = str(values_[date_])


    content, date_index, value_index=read_data_file(url, column)
    for i in range (1, len(content)):
        if content[i]=="":
            continue
        lin=content[i].split(',')
        if lin[0]=="":
            continue
        date__=datetime.datetime.strptime(lin[date_index], date_format)
        values[str(date__)]=lin[value_index]
'''
    #return json.dumps({'0' :values})

    return json.dumps({'0': values_})


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
        print "vaues____:", (values_.keys())
        keys_=values_.keys()
        for v in values_['values']:
            dat=datetime.datetime(year, month, day)
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
    metadata = {}
    metadata['single'] = 'yes'
    print (type(value_),"_toz=====>", value_)
    try:
        print "Value type:===>>", value_['type']
        print name
    except:
        print "No type found"
    print name
    print _type
    print "Res attr: ",type(res_attributes), get_dict(res_attributes)
    print "======================================================================"

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
            print name, value_
    except:
        print "It is failed ......"
        print name
        print value_
        if isinstance(value_, list):
            print "It is failed ......1"
            attr_type = "array"
            value = json.dumps(value_)
        elif isinstance(value_, basestring):
            print "It is failed ......2", name, value_
            attr=is_in_dict(value_, ref_parameters)
            if attr != None:
                print "Reference is foun ..."
                print get_dict((attr))
            #if(value_ in ref_parameters.keys()):
                #toz   toz
                #attr = ref_parameters[value_]
                attr_type = attr.type
                value = attr.value
                metadata=json.loads(attr.metadata)
                print "Metadata: ", metadata
                if( 'type' in metadata.keys()):
                    if(metadata['type']=='aggregated'):
                        print "From aggregated section"
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
            print "It is failed ......3"
            metadata['type'] = value_['type'].lower()
            if value_['type'].lower() =='constant':
                value = str(value_['value'])
                attr_type = 'scalar'
            elif value_['type'].lower() == "arrayindexed" or value_['type'].lower() == "dataframe" or value_['type'].lower() == "tablesarray":
                metadata['type'] = value_['type']
                if value_['type'].lower()=='tablesarray':
                    metadata['where']=value_['where']
                    metadata['node'] = value_['node']
                    value=read_tablesarray(value_['url'], value_['where'] ,value_['node'])
                    attr_type = 'array'
                else:
                    print "------------------------>", value_.type.lower()
                    attr_type = 'timeseries'
                    sheetname=None
                    if 'sheetname' in keys_:
                        sheetname=value_['sheetname']
                    if  "column" in value_:
                        value = read_timeseries(value_['url'],sheetname,  name, value_['column'])
                        metadata['column']=value_['column']
                    else:
                        value = read_timeseries(value_['url'], name, sheetname)
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
                print "It is aggregated .."
                get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'indexedarray':
                get_indexedarray_attributes(value_, name, counter, attributes_, _type,  res_attributes)

                return
            elif value_['type'].lower() == 'controlcurveindex':
                print "== == == == == == = >> >> > controlcurveindex"
                get_controlcurveindex_attributes(value_, name, counter, attributes_, _type, res_attributes)
                print "DONEW .......", res_attributes
                return
            elif value_['type'].lower() =='monthlyprofilecontrolcurve':
                print ("It is monthlyprofilecontrolcurve")
                get_monthlyprofilecontrolcurve_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'controlcurveinterpolated':

                get_controlcurveinterpolated_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_['type'].lower() == 'recorderthreshold':
                get_recorderthreshold(value_, name, counter, attributes_, _type, res_attributes)
                return

    if "comment" in keys_:
        metadata['comment']=value_['comment']
    print "____value====>>>>", value_
    counter.id = counter.id - 1
    attributes_[name] = AttributeData(attr_type, value, '-', 'Dimensionless', metadata)
    if(res_attributes!=None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name , attributes_[name],'Dimensionless'))


def is_in_dict(key_, dict_):
    for k in dict_.keys():
        if k.strip().lower()== key_.strip().lower():
            return dict_[k]
    return None

def get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes=None):
    print "hello from It is aggregated .."
    print json.dumps(value_['parameters'])
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
    print "From aggg function ....."
    for para in paras:
        print "Parameter: ", para
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
    print " == == = >> >> > controlcurveindex", value_
    if "storage_node" in value_:
        print "Storage node is found ", value_["storage_node"]
        metadata['storage_node']=value_["storage_node"]
    if "comment" in value_:
        metadata['comment'] = value_['comment']
    value=value_['control_curves']
    for item in value:
        get_attribute_type_and_value(item, item, counter, attributes_, _type, res_attributes)
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    counter.id = counter.id - 1
    attributes_[name] = attr
    print name, "Attr ibut is: ",get_dict(attr)
    print "recourseAttributea", recourseAttributea
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
    print value_, "=====================>>>>>"
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

def get_new_attributes(attrlist, c_attrlist):
    new_attr=[]
    for attr in attrlist:
        for eattr in c_attrlist:
            if(attr.name.lower() == eattr.name.lower() and attr.dimen.lower() == eattr.dimen.lower):
                break
        new_attr.append(get_dict(attr))
    return new_attr

def get_dict(obj):
    if not  hasattr(obj,"__dict__"):
        return obj
    '''
    if isinstance(obj, list):
        print "IT IS A LIST ====>>>>>>>>"
        for item in obj:
            get_dict(item)
    '''
    result = {}
    for key, val in obj.__dict__.items():

        if key.startswith("_"):
            continue
        if isinstance(val, list):
            element = []
            for item in val:
                element.append(get_dict(item))
        else:
            element = get_dict(obj.__dict__[key])
        result[key] = element
    return result


def check_in_a_list(json_list):
    for item in json_list:
        if (type(item) is list):
            check_in_a_list(item)
        elif (type(item) is dict):
            for kk in item.keys():
                print kk
                if (type(item[kk]) is list):
                    check_in_a_list(item[kk])
                else:
                    print kk, item[kk]
        else:
            print ((item))


def test_for_invalid_keys_names(json_string):
    main_json = json.loads(json_string)
    for key in main_json.keys():
        print "key is: ", key
        if (type(main_json[key]) is list):
            for item in main_json[key]:
                check_in_a_list(item)
        elif (type(main_json[key]) is dict):
            print main_json[key]

    import sys
    #sys.exit(0)

def import_net (filename, connector):
        has_tablerecorder = False
        json_list=[]
        json_file__folder.append(os.path.dirname(filename))
        counter=Counter()
        recorders=[]
        domains=[]
        project_attributes = {}
        pp= dict(
        name="CSV import at %s" ,
        description= \
            "Project created by the %s plug-in, %s." ,
        status='A',
        )
        c_attrlist = connector.call('get_all_attributes', {})
        nodes_attributes={}
        links_attributes = {}
        network_attributes={}
        #with open(filename) as f:
        #    json_string = f.read().splitlines()
        #'''
        f = open(filename,'r')
        json_string = ""
        while 1:
            line = f.readline()
            if not line:break
            json_string += line
        f.close()
        #'''
        test_for_invalid_keys_names(json_string)
        main_json = json.loads(json_string)#, object_hook=lambda d: namedtuple('X', d.keys(), rename=False, verbose=False)(*d.values()))
        #xx=main_json
        json_list.append(main_json)
        if "includes" in main_json:
                for included_file in main_json['ncludes']:
                    ss = os.path.dirname(included_file )
                    if (ss == ''):
                        included_file = os.path.join(json_file__folder[0], included_file)
                    included_f = open(included_file, 'r')
                    included_json_string = ""
                    while 1:
                        line = included_f.readline()
                        if not line: break
                        included_json_string += line
                    included_f.close()

                    included_x = json.loads(included_json_string , object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
                    json_list.append(included_x)



                    if ("nodes" in included_x):
                        for node in included_x['nodes']:
                            main_json.nodes.append(node)

                    if "edges" in included_x:
                        for edge in included_x['edges']:
                            main_json.edges.append(edge)
                    #if hasattr(included_x, "parameters"):
                    #    for key_ in included_x.parameters:
                    #        if hasattr(main_json, "parameters"):
                    #           main_json.parameters[key_]=included_x.parameters[key_]

        global timeseries
        if "timestepper" in main_json:
            timeseries=get_timeseriesdates(main_json['timestepper'])
        project=Project()
        request=jsonpickle.dumps(project).replace("/","_").replace("\"", '\'')
        Proj = connector.call('add_project', {'project': project.__dict__})
        if ("recorders" in main_json):
            rcds=get_dict(main_json['recorders'])
            print "SOSOSOSOS"
            print rcds.keys()
            for kk in rcds.keys():
                print type(rcds[kk])
                print rcds[kk]
                recorders.append(Recorder(kk, rcds[kk]))

        if ( "domains" in main_json):
            for domain_ in main_json['domains']:
                domains.append(Domain(domain_))

        timestepper=None
        if ("timestepper" in main_json):
            timestepper = main_json['timestepper']

        solver_name=None
        if ("solver" in main_json):
            solver_name=main_json['solver']

        if ("metadata" in main_json):
            network = Network('Pywr',solver_name, Proj.id, counter, domains, recorders, network_attributes,
                              timestepper, main_json['metadata'])
        else:
            network = Network('Pywr', solver_name, Proj.id, counter, domains, recorders, network_attributes,
                              timestepper)

        for attr_name in network_attributes[network.name].keys():
            if (attr_name in project_attributes):
                pass
            else:
                attr = Attribute(attr_name)
                project_attributes[attr_name] = attr

        counter.id=counter.id-1;
        nodes_ids={}
        i = 0
        for json_ in json_list:
            print "Parameters section is found...."
            print json_['parameters'].keys()
            if ("parameters" in json_):
                for i in range(0, len(json_['parameters'].keys())):
                    k = json_['parameters'].keys()[i]
                    print k
                    print "1",  json_['parameters'].values()[i]
                    print "2", json_['parameters'][k]

                    counter.id = counter.id - 1
                    print "1", len(ref_parameters)
                    get_attribute_type_and_value(json_['parameters'].values()[i], k, counter, ref_parameters, 'default')
                    print "2", len(ref_parameters)

        for node_ in main_json['nodes']:
            node_records=[]
            print "recorders", (get_dict(recorders))
            for rec in recorders:
                 print "REC: ", get_dict(rec)
                 if hasattr (rec, "node"):
                 #if ("node" in rec):
                    if rec.node==node_['name']:
                         node_records.append(rec)

            node = Node(node_, counter, nodes_attributes, node_records)
            nodes_ids[node_['name']] = node.id
            counter.id = counter.id - 1
            network.nodes.append(node)
            for attr_name in nodes_attributes[node.name].keys():
                if( attr_name in project_attributes):
                    pass
                else:
                    attr=Attribute(attr_name)
                    project_attributes[attr_name]=attr

        for edge_ in main_json['edges']:
           link=Link(edge_, nodes_ids, counter, links_attributes)
           counter.id = counter.id - 1
           network.links.append(link)
           for attr_name in links_attributes[link.name].keys():
               if (attr_name in project_attributes):
                   pass
               else:
                   attr = Attribute(attr_name)
                   project_attributes[attr_name] = attr

        new_attr = get_new_attributes(project_attributes.values(), c_attrlist)
        attributes = connector.call('add_attributes', {'attrs': new_attr})
        attrs_names_ids={}
        for tt in attributes:
            attrs_names_ids[tt.name]=tt.id
        for rs in recourseAttributea:
            try:
                rs.attr_id=attrs_names_ids[rs.attr_id]
            except:
                print "Error-------------->", rs.attr_id
        network.scenarios[0].resourcescenarios=recourseAttributea

        for rs in network.attributes:
            rs.attr_id = attrs_names_ids[rs.attr_id]
        for node in network.nodes:
            for rs in node.attributes:
                rs.attr_id=attrs_names_ids[rs.attr_id]
        for link in network.links:
            for rs in link.attributes:
                rs.attr_id = attrs_names_ids[rs.attr_id]
        print "ref: ", ref_parameters.keys()

        print "project: ", project_attributes.keys()

        NetworkSummary = connector.call('add_network', {'net': get_dict(network)})
        #print ("NETWORK:/n", get_dict(network))
        return NetworkSummary