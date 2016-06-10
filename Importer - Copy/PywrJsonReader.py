import json
import jsonpickle
import random
from collections import namedtuple
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import datetime
import inspect


#"Dimensionless"

class Counter(object):
    def __init__(self):
        self.id=-1

recourseAttributea =[]
ref_parameters={}

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
        print "type is: ", type

class RecourseAttribute (object):
    def __init__(self,  ref_key, resource_attr_id, attr_id, value_, dimen):
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.source=ref_key
        self.value=value_

class Node(object):
    def __init__(self, node_, counter, nodes_attributes):
        i=0
        self.id = counter.id
        self.status = "A"
        self.name = node_.name
        self.description = ""
        self.x = str(random.randint(0,99))
        self.y = str(random.randint(0,99))
        self.type = node_.type
        attributes_ = {}
        self.attributes = []
        ras=[]
        print self.name
        for i in range(0, len(node_.__dict__.keys())):
            k=node_.__dict__.keys()[i]
            if(k.lower() != 'name' and k.lower() !='type'):
                counter.id=counter.id-1
                attributes_[k] =  get_attribute_type_and_value(node_.__dict__.values()[i])
                self.attributes.append(ResourceAttr(counter.id, k, self.type))
                recourseAttributea.append(RecourseAttribute('NODE', counter.id, k , attributes_[k],'Dimensionless'))
        nodes_attributes [self.name]= attributes_

class Link(object):
    def __init__(self, edge_, nodes_ids, counter):
        self.node_1_id = nodes_ids[edge_[0]]
        self.node_2_id = nodes_ids[edge_[1]]
        self.name = edge_[0] + '_' + edge_[1]
        self.id = counter.id
        self.status = "A"
        self.attributes={}
        self.description=""

class Network (object):
    def __init__(self, name, solver_name, project_id):
        self.project_id=project_id
        self.name=name+'_'+str(datetime.datetime.now())
        self.id=-1
        #solver_attribute=Attribute('solver')
        self.attributes = {}
        #self.attributes[solver_attribute]= solver_name
        self.nodes=[]
        self.links=[]
        self.scenarios=[Scenario()]
        self.description = ""
        self.description = ""

class Scenario(object):
    def __init__(self):
        self.description="Created by Pywr exporter"
        self.name="scenarion_" + str(datetime.datetime.now())
        self.resourcescenarios=[]

class Resourcescenario(object):
    def __init__(self, source, resource_attr_id, attr_id, value):
        self.source=source
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.value=value

class AttributeData (object):
    def __init__(self,  type, value, unit, dimen):
        self.hidden='N'
        self.type=type
        self.name = 'pywerApp'
        self.unit=unit
        self.dimension=dimen
        self.metadata='{}'
        self.value=value
        #self.metadata=metadata


def get_timeseriesdates(timestepper):
    start=parse(timestepper.start)
    end=parse(timestepper.end)
    timestep=(timestepper.timestep)
    timeseries=[]
    while (end>=start):
        timeseries.append(start)
        start = start + timedelta(days=timestep)

def read_timeseries(url):
    print url
    return None
    with open(url) as file:
        content = file.read().splitlines()
    head=content[0].split(',')
    if( head[0].lower()== 'date'):
        date_index=0
        value_index=1
    else:
        date_index = 1
        value_index = 0
    values={}
    for i in range (1, len(content)):
        lin=content[i].split(',')
        values[lin[date_index]]=lin[value_index]
    return json.dumps({'0' :values})

def read_seasonall(values_):
    values={}
    year=9999
    month=1
    day=1
    for v in values_:
        dat=datetime.datetime(year, month, 24)
        values[str(dat)]=v
        month+=1
    print values
    return json.dumps({'0': values})
    #return values

def get_attribute_type_and_value(value_):
    try:
        float(value_)
        value=str(value_)
        type='scalar'
    except:
        print isinstance(value_, list)
        if isinstance(value_, list):
            type="array"
            value=json.dumps(value_)
        elif isinstance(value_, basestring):
            attr=ref_parameters[value_]
            type=attr.type
            value=attr.value
        elif value_.type.lower().startswith("arrayindexed"):
            type= 'timeseries'
            value=read_timeseries(value_.url)
        elif value_.type.lower().startswith("monthly"):
            type = 'timeseries'
            value = read_seasonall(value_.values)

    return AttributeData(type, value, '-', 'Dimensionless')


def get_new_attributes(attrlist, c_attrlist):
    new_attr=[]
    for attr in attrlist:
        for eattr in c_attrlist:
            if(attr.name.lower() == eattr.name.lower() and attr.dimen.lower() == eattr.dimen.lower):
                break
        new_attr.append(dumper(attr))
    return new_attr

def dumper(obj):
    try:
        return obj.toJSON()
    except:
        return obj.__dict__

def create_resources_scenario(network, attrs_names_ids):
    pass


def get_dict(obj):
    if not  hasattr(obj,"__dict__"):
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


def export (filename, c_attrlist, connection):
        counter=Counter()
        pp= dict(
        name="CSV import at %s" ,
        description= \
            "Project created by the %s plug-in, %s." ,
        status='A',
        )
        nodes_attributes={}
        f = open(filename,'r')
        json_string = ""
        while 1:
            line = f.readline()
            if not line:break
            json_string += line
        f.close()
        x = json.loads(json_string, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        timeseries=get_timeseriesdates(x.timestepper)
        project=Project()
        request=jsonpickle.dumps(project).replace("/","_").replace("\"", '\'')
        Proj = connection.call('add_project', {'project': project.__dict__})
        network = Network('Pywr', x.solver, Proj.id)
        counter.id=-2
        nodes_ids={}
        project_attributes={}
        i=0
        if hasattr(x,"parameters"):
            for i in range(0, len(x.parameters.__dict__.keys())):
                k = x.parameters.__dict__.keys()[i]
                counter.id = counter.id - 1
                attribute = get_attribute_type_and_value(x.parameters.__dict__.values()[i])
                ref_parameters[k]=attribute
                #recourseAttributea.append(RecourseAttribute('REF', counter.id, k, attribute, 'Dimensionless'))

        for node_ in x.nodes:
            node = Node(node_, counter, nodes_attributes)
            nodes_ids[node_.name] = node.id
            counter.id = counter.id - 1
            network.nodes.append(node)
            for attr_name in nodes_attributes[node.name].keys():
                if( attr_name in project_attributes):
                    pass
                else:
                    attr=Attribute(attr_name)
                    project_attributes[attr_name]=attr

        for edge_ in x.edges:
           link=Link(edge_, nodes_ids, counter)
           counter.id = counter.id - 1
           network.links.append(link)
        new_attr = get_new_attributes(project_attributes.values(), c_attrlist)
        attributes = connection.call('add_attributes', {'attrs': new_attr})
        attrs_names_ids={}
        for tt in attributes:
            print "attribute name: ", tt.name
            attrs_names_ids[tt.name]=tt.id
        for rs in recourseAttributea:
            if(rs.attr_id in attrs_names_ids.keys()):
                rs.attr_id=attrs_names_ids[rs.attr_id]
            else:
                print "It is not there ..",rs, rs.attr_id
        network.scenarios[0].resourcescenarios=recourseAttributea
        for node in network.nodes:
            for rs in node.attributes:
                rs.attr_id=attrs_names_ids[rs.attr_id]
        for node in network.nodes:
            print get_dict(node)
        #struct=Struct(network)
        #print "================================================================================================================="
        #print (get_dict(network))
        #print "================================================================================================================="
        NetworkSummary = connection.call('add_network', {'net': get_dict(network)})
        return NetworkSummary

if __name__ == '__main__':
    array=[3, 2]
    print array
    array2=[array, [4, 3]]
    print len (array2)
    print len(array)
    for ar in array2:
        print ar
    export('pywr.json')
