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
        for attr_ in node_.attributes:
            attr=attributes_ids[attr_.attr_id]
            #This needs to be checked !!!!!!!!!!!!!!!
            res=resourcescenarios_ids[attr_.id]
            self.position=[node_.x, node_.y]

            if(attr.name=='node_type'):
                self.type=res.value.value
            elif res.value.type == 'descriptor' or res.value.type=='scalar':
                self[attr.name]=res.value.value
                attributes[attr.name] = self[attr.name]
            elif res.value.type == 'timeseries':
                self[attr.name] = get_timesreies_values(res.value.value, attr.name)
                attributes[attr.name]=self[attr.name]
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

class Link (object):
    def __init__(self, link_):
        pass

class Parameter(object):
    def __init__(self):
        pass

class Value(object):
    def __init__(self):
        pass

class Recorder(object):
    def __init__(self):
        pass

class Domain(object):
    def __init__(self):
        pass

class Timestepper(object):
    def __init__(self):
        pass

class Metadata (object):
    def __init__(self):
       pass

def get_dict(obj):
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
                                        print ' I have found array'
                                        if (not attr+'_ref' in parameters):
                                            parameters[attr]={'type': attrs[attr] ['type'], 'values':attrs_[attr_]['values']}
                        else:
                            if attrs[attr] == attrs_[attr_]:
                                print attr in parameters
                                if(not attr+'_ref' in parameters ):
                                    parameters[attr]=attrs[attr]

    for node in nodes:
        for key in node:
            for attr in parameters:
                    if attr == key and parameters[attr]== node[key]:
                        node[key]=attr+'_ref'
                        print node


    return  parameters

def pywrwriter (network, attrlist):

    #print network
    nodes=[]
    edges=[]
    nodes_parameters={}
    recorders=[]
    parameters=[]
    domains=[]
    attributes_ids={}
    metadata =Metadata ()
    for attr in attrlist:
        attributes_ids[attr.id]=attr
    resourcescenarios_ids=get_resourcescenarios_ids(network.scenarios[0].resourcescenarios)

    for node_ in network.nodes:
        node=Node(node_, attributes_ids, resourcescenarios_ids, nodes_parameters)
        nodes.append(node)
    parameters=get_parameters_refs(nodes, nodes_parameters)

    for link_ in network.links:
        link=Link(link_)
        edges.append(link)

    print get_dict(metadata)



