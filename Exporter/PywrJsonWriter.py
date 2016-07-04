import json
import jsonpickle
import random
from collections import namedtuple
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import datetime
import inspect


class Node(dict):
    def __init__(self, node_, attributes_ids, resourcescenarios_ids, nodes_parameters):
        self.name=node_.name
        attributes={}
        geographic=None
        for attr_ in node_.attributes:
            attr=attributes_ids[attr_.attr_id]
            #This needs to be checked !!!!!!!!!!!!!!!
            res=resourcescenarios_ids[attr_.id]
            if(attr.name=='node_type'):
                self.type=res.value.value
            elif attr.name == 'geographic':
                geographic = json.loads(res.value.value)
            elif res.value.type == 'descriptor' or res.value.type=='scalar':
                self.__dict__[attr.name]=res.value.value
                attributes[attr.name] = self.__dict__[attr.name]
            elif res.value.type == 'timeseries':
                self.__dict__[attr.name] = get_timesreies_values(res.value.value, attr.name)
                attributes[attr.name]=self.__dict__[attr.name]


        self.position = {}

        self.position['schematic'] = [node_.x, node_.y]
        if(geographic==None):
            self.position['geographic'] = []
        else:
            self.position['geographic'] = geographic

        nodes_parameters[self.name]=attributes

def get_timesreies_values(value, column):
    values={}
    vv = json.loads(value)
    contents=[]
    contents.append('Date, '+column+'\n')
    for key in vv.keys():
        for date in sorted(vv[key].keys()):
            if(date.startswith('9999')):
                if(len(vv[key])==12):
                    values['type'] = 'monthlyprofile'
                elif (len(vv[key])==365):
                    values['type'] = 'dailyyprofile'
                else:
                    values['type'] = 'default'
                values['values'] = ger_arrayvalues(vv[key])
                return  values
            contents.append(date+','+str(vv[key][date])+'\n')
    values['type'] = 'arrayindexed'
    values['url']=contents
    values['column']=column
    return values


def write_time_series_tofile(contents, filename):
    file = open(filename, "w")
    file.write("".join(contents))
    file.close()

def ger_arrayvalues(value_):
    contenets=[]
    for date in sorted(value_.keys()):
        contenets.append(value_[date])
    return contenets

def get_resourcescenarios_ids(resourcescenarios):
    resourcescenarios_ids={}
    for res in resourcescenarios:
        resourcescenarios_ids[res.resource_attr_id]=res
    return resourcescenarios_ids

class Edge(object):
    def __init__(self, link_, attributes_ids, resourcescenarios_ids, nodes_id_name):
        self.attrs=[nodes_id_name[link_.node_1_id], nodes_id_name[link_.node_2_id]]
        for attr_ in link_.attributes:
            attr = attributes_ids[attr_.attr_id]
            res = resourcescenarios_ids[attr_.id]
            if (attr.name == 'slot_from'):
                if(res.value.value=='None'):
                    slot_from = None
                else:
                    slot_from = res.value.value
            elif (attr.name == 'slot_to'):
                if(res.value.value=='None'):
                    slot_to = None
                else:
                    slot_to = res.value.value

        if(slot_to !=None or slot_from !=None):
            self.attrs.append(slot_from)
            self.attrs.append(slot_to)

class Parameter(object):
    def __init__(self):
        pass

class Value(object):
    def __init__(self):
        pass

class Recorder(object):
    def __init__(self, value):

        if(len(value)==2):
            self.node=value[1]
            self.type=value[0]
        elif (len(value)==3):
            try:
                recs=json.loads(value[1])
                self.type=value[0]
                self.recorders=recs
                self.agg_func=value[2]
            except:
                self.name=value[2]
                self.node = value[1]
                self.type = value[0]

def get_recotds(network, attributes_ids, resourcescenarios_ids):
    recorders=[]
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        res = resourcescenarios_ids[attr_.id]
        if (attr.name == 'recorders'):
            values=json.loads(res.value.value)
            for value in values:
                recorders.append(Recorder(value))
    return recorders


class Domain(object):
    def __init__(self, name, color):
        self.name = name
        self.color = color


class Solver(object):
    def __init__(self, link_, attributes_ids, resourcescenarios_ids):
        for attr_ in link_.attributes:
            attr = attributes_ids[attr_.attr_id]
            # This needs to be checked !!!!!!!!!!!!!!!
            res = resourcescenarios_ids[attr_.id]
            if (attr.name == 'solver'):
                self.name=res.value.value

class Timestepper(object):
    def __init__(self, network, attributes_ids, resourcescenarios_ids):
        for attr_ in network.attributes:
            attr = attributes_ids[attr_.attr_id]
            res = resourcescenarios_ids[attr_.id]
            if (attr.name == 'start_time'):
                self.start = res.value.value
            elif (attr.name == 'end_time'):
                self.end = res.value.value
            elif (attr.name == 'timestep'):
                self.timestep = res.value.value

class Metadata (object):
    def __init__(self, network, resourcescenarios_ids, attributes_ids):
       self.title=network.name
       for attr_ in network.attributes:
           attr = attributes_ids[attr_.attr_id]
           res = resourcescenarios_ids[attr_.id]
           if (attr.name == 'author'):
               self.author = res.value.value
       self.description=network.description

def get_dict(obj):
    if type(obj) is list:
        list_results=[]
        for item in obj:
            list_results.append(get_dict(item))
        return list_results

    if not hasattr(obj, "__dict__"):
        return obj

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

class PywrNetwork (object):
    def __init__(self, metadata, timestepper, solver, nodes, edges, domains, parameters, recorders):
        self.parameters=parameters
        self.recorders=recorders
        self.domains=domains
        self.nodes=nodes
        self.edges=edges
        self.solver=solver
        self.timestepper=timestepper
        self.metadata=metadata

def get_parameters_refs(nodes, nodes_parameters):
    parameters = {}
    for i in range(0, len(nodes)):
        node = nodes[i]
        for j in range(i+1, len(nodes)):
            node_=nodes[j]
            attrs=nodes_parameters[node.name]
            attrs_ = nodes_parameters[node_.name]
            for attr in attrs.keys():
                for attr_ in attrs_.keys():
                    if(attr==attr_):
                        if hasattr(attrs[attr], "type"):
                            if attrs[attr] ['type']== attrs_[attr_]['type']:
                                if(attrs[attr] ['type']=='arrayindexed'):
                                    if attrs[attr]['url'] == attrs_[attr_]['url']:
                                        if (not attr+'_ref' in parameters):
                                            parameters[attr] = {'type': attrs[attr]['type'],
                                                            'url': attrs_[attr_]['url']}
                                else:
                                    print attrs[attr]['values']
                                    print attrs_[attr_]['values']
                                    print 'Compare->: ', (set(attrs[attr]['values']) & set(attrs_[attr_]['values']))
                                    if attrs[attr]['values'] == attrs_[attr_]['values']:
                                        if (not attr+'_ref' in parameters):
                                            parameters[attr]={'type': attrs[attr] ['type'], 'values':attrs_[attr_]['values']}
                        else:
                            if attrs[attr] == attrs_[attr_]:
                                if(not attr+'_ref' in parameters ):
                                    parameters[attr+'_ref' ]=attrs[attr]


    for node in nodes:
        for key in node.__dict__.keys():
            for attr in parameters.keys():
                if attr == key+'_ref':
                    if parameters[attr]== node.__dict__[key]:
                        node.__dict__[key]=attr

    return  parameters


def pywrwriter (network, attrlist, output_file):
    #print network
    nodes=[]
    edges=[]
    nodes_parameters={}
    parameters=[]
    domains=[]
    attributes_ids={}
    for attr in attrlist:
        attributes_ids[attr.id]=attr
    resourcescenarios_ids=get_resourcescenarios_ids(network.scenarios[0].resourcescenarios)
    timestepper=Timestepper(network, attributes_ids, resourcescenarios_ids)
    metadata = Metadata(network, resourcescenarios_ids, attributes_ids)
    recorders=get_recotds(network, attributes_ids, resourcescenarios_ids)
    domains=[]
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        # This needs to be checked !!!!!!!!!!!!!!!
        res = resourcescenarios_ids[attr_.id]
        if (attr.name == 'domains'):
            val = json.loads(res.value.value)
            name = val[0][0]
            color = val[0][1]
            domains.append(Domain(name, color))
    solver = Solver(network, attributes_ids, resourcescenarios_ids)
    nodes_id_name={}
    for node_ in network.nodes:
        node=Node(node_, attributes_ids, resourcescenarios_ids, nodes_parameters)
        nodes_id_name[node_.id]=node_.name
        nodes.append(node)
    parameters=get_parameters_refs(nodes, nodes_parameters)

    for link_ in network.links:
        edge=Edge(link_, attributes_ids, resourcescenarios_ids, nodes_id_name)
        edges.append(edge.attrs)
    for node in nodes:
        for i in range(0, len(node.__dict__.keys())):
            k = node.__dict__.keys()[i]
            if (k.lower() != 'name' and k.lower() != 'type' and k.lower() != 'position'):
                value=node.__dict__.values()[i]
                if type(value) is dict:
                    if value['type'] == 'arrayindexed':
                        file_name=node.name+"_"+k+'.csv'
                        write_time_series_tofile(value['url'], file_name)
                        value['url']=file_name

    pywrNetwork=PywrNetwork(metadata, timestepper, solver, nodes, edges, domains, parameters, recorders)

    with open(output_file, "w") as text_file:
        text_file.write(json.dumps(get_dict(pywrNetwork), indent=2))






