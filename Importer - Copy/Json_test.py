import json
import random
from collections import namedtuple
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import inspect


#"Dimensionless"

class Attribute (object):
    def __init__(self,name, desc=None):

        self.name=name
        self.status='A'
        self.description=''

class RecourseAttribute (object):
    def __init__(self, type, ref_key, ref_id, attr_id, value_, dimen, unit):
        if(type.lower() == 'output'):
            self.attr_is_var='Y'
        else:
            self.attr_is_var = 'N'
        self.ref_key=ref_key
        self.ref_id=ref_id
        self.attr_id=attr_id
        self.value={'dimension':dimen, 'unit':unit, 'value':value_}

class Node(object):
    def __init__(self, node_, id, nodes_attributes):
        i=0;
        self.id = id;
        self.status = "A"
        self.name = node_.name
        self.x = random.random()
        self.y = random.random()
        self.type = node_.type
        attributes_ = {}
        self.attributes = []
        print self.name
        for i in range(0, len(node_.__dict__.keys())):
            k=node_.__dict__.keys()[i]
            if(k.lower() != 'name' and k.lower() !='type'):
                attributes_[k] =  get_attribute_type_and_value(node_.__dict__.values()[i])
                self.attributes.append(RecourseAttribute(self.type, 'NODE', self.id,'0', attributes_[k],'Dimensionless','-' ))
        nodes_attributes [self.name]= attributes_

class Link(object):
    def __init__(self, edge_, nodes_ids, id):
        self.node_1_id = nodes_ids[edge_[0]]
        self.node_2_id = nodes_ids[edge_[1]]
        self.name = edge_[0] + '_' + edge_[1]
        self.id = id
        self.status = "A"
        self.attributes={}

class Network (object):
    def __init__(self, name, solver_name):
        self.name=name
        self.id=-1
        solver_attribute=Attribute('solver')
        self.attributes = {}
        self.attributes[solver_attribute]= solver_name
        self.nodes=[]
        self.links=[]


class Scenario(object):
    def __init__(self):
        pass

def get_timeseriesdates(timestepper):
    start=parse(timestepper.start)
    end=parse(timestepper.end)
    timestep=(timestepper.timestep)
    timeseries=[]
    while (end>=start):
        timeseries.append(start)
        start = start + timedelta(days=timestep)


def read_timeseries(url):
    with open(url) as f:
        content = f.read().splitlines()
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

def get_attribute_type_and_value(value_):
    try:
        value= float(value_)
        type='scalar'
    except:
        type= 'timeseries'
        value=read_timeseries(value_.url)

    return {'type': type, 'value':value}



def export (filename):
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
        network = Network('Pywr', x.solver)
        id=-2;
        nodes_ids={}
        project_attributes={}
        print x.timestepper.start, x.timestepper.end, x.timestepper.timestep

        for node_ in x.nodes:
            node = Node(node_, id, nodes_attributes)
            nodes_ids[node_.name] = id
            id = id - 1
            network.nodes.append(node)
            for attr_name in nodes_attributes[node.name].keys():
                if( attr_name in project_attributes):
                    pass
                else:
                    attr=Attribute(attr_name)
                    project_attributes[attr_name]=attr_name

        for edge_ in x.edges:
           link=Link(edge_, nodes_ids, id)
           id = id - 1
           network.links.append(link)




if __name__ == '__main__':
    array=[3, 2]
    print array
    array2=[array, [4, 3]]
    print len (array2)
    print len(array)
    for ar in array2:
        print ar
    #export('pywr.json')
