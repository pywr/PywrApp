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
    """
    Class contains the essential Hydra project elements
    """
    def __init__(self):
        self.name="Pywer exported project at "+str(datetime.now())
        self.status = "A"
        self.description="Create by Pywr exporter"
        self.id=-1


class Value (object):
    """
      Attribute value
    """
    def __init__(self, value):
        self.value=value
        self.dimen="Dimensionless"
        self.unit="-"

class Attribute (object):
    def __init__(self,name):
        self.name=name
        self.status="A"
        self.description="-"
        self.dimen="dimensionless"
        self.id=-1

class ResourceAttr (object):
    def __init__(self, id, attr_id, type):
        self.id=id
        self.attr_id=attr_id
        if (type.lower() == "output"):
            self.attr_is_var = "Y"
        else:
            self.attr_is_var = "N"

class RecourseAttribute (object):
    def __init__(self,  ref_key, resource_attr_id, attr_id, value_, dimen):
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.source=ref_key
        self.value=value_

class Node(object):
    def __init__(self, node_, counter, nodes_attributes):
        global  has_tablerecorder
        is_flow=True
        attributes_ = {}
        self.attributes = []
        ras = []
        x=None
        y=None
        i=0
        self.id = counter.id
        self.status = "A"
        self.name = node_["name"]
        self.type = node_["type"]
        self.description = ""
        if hasattr(node_, "position"):
            if hasattr(node_.position, "position"):
                if(node_.position.schematic != None and len (node_.position.schematic)==2):
                    x=str(node_.position.schematic[0])
                    y=str(node_.position.schematic[1])
        if(x != None):
            self.x = str(x)
        else:
            self.x='0'
        if(y!=None):
            self.y = str(y)
        else:
            self.y='0'
        keys_=sorted(node_.keys())
        for i in range(0, len(keys_)):#.__dict__.keys())):
            k=keys_[i]
            if(k.lower() != "name" and k.lower() !="type" and k.lower() !="position"):
                val=str(node_[k])
                if k=='comment':
                    self.description=val
                    continue
                counter.id = counter.id - 1
                attributes_[k] = AttributeData("descriptor", val, "-", "Dimensionless", k,
                                                 {"pywr_section":'nodes'})
                self.attributes.append(ResourceAttr(counter.id, k, "Input"))
                recourseAttributea.append(
                RecourseAttribute("NODE", counter.id, k, attributes_[k], "Dimensionless"))



                #get_attribute_type_and_value(val, k, counter, attributes_, "input", self.attributes)

        #get_attribute_type_and_value(node_["type"], "node_type", counter, attributes_, "input", self.attributes)
        nodes_attributes [self.name]= attributes_

class Link(object):
    """
    Hydra link from pywr edge
    """
    def __init__(self, edge_, nodes_ids, counter, links_attributes):
        self.node_1_id = nodes_ids[edge_[0]]
        self.node_2_id = nodes_ids[edge_[1]]
        self.name = edge_[0] + "_" + edge_[1]
        self.description=""
        self.attributes = []
        self.type="edge"
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
        if slot_from != None:
            counter.id = counter.id - 1
            attributes_["slot_from"]=AttributeData("descriptor", str(slot_from), "-", "Dimensionless","slot_from", {"pywr_section":'links'})
            self.attributes.append(ResourceAttr(counter.id, "slot_from","Input"))
            recourseAttributea.append(RecourseAttribute("LINK", counter.id, "slot_from", attributes_["slot_from"], "Dimensionless"))
        if slot_to != None:
            counter.id = counter.id - 1
            attributes_["slot_to"]=AttributeData("descriptor", str(slot_to), "-", "Dimensionless", "slot_to", {"pywr_section":'links'})
            self.attributes.append(ResourceAttr(counter.id, "slot_to", "Input"))
            recourseAttributea.append(RecourseAttribute("LINK", counter.id, "slot_to", attributes_["slot_to"], "Dimensionless"))
        links_attributes[self.name]=attributes_

class Network (object):
    """
     Create Hydra network
    """
    def __init__(self, name, solver_name, project_id, counter, domains, recorders, network_attributes, timestepper,  main_Json):
        """
        :param name: network name
        :param solver_name: pywr solver name, it will be added as network attribute
        :param project_id: Hydra project id
        :param counter: object contains id which is used to estimate attribute and scenario attributes ids
        :param domains: doamin section inside pywr, it will be saved as network attributes
        :param recorders: conatins pywr recorder section, some of them will be saved as network attributes
        :param network_attributes: other network attributes
        :param timestepper: pywr timestepper
        :param metadata: pywr metadata
        """
        #print "recorders:=>>", get_dict(recorders)
        global has_tablerecorder

        self.attributes = []
        global scenarios
        attributes_={}
        self.name="Pywr_network"
        self.project_id=project_id
        self.type="PywrNetwork"
        author=None
        minimum_version=None
        for key in main_Json:
            if key =="nodes" or key=="edges":
                continue
            if key=="recorders" or key=="parameters":
                for pars in main_Json[key]:
                    counter.id = counter.id - 1
                    attributes_[pars] = AttributeData("descriptor", main_Json[key][pars], "-", "Dimensionless",pars, {"pywr_section": key})
                    self.attributes.append(ResourceAttr(counter.id, pars, "Input"))
                    recourseAttributea.append(
                        RecourseAttribute("NETWORK", counter.id, pars, attributes_[pars], "Dimensionless"))
            else:
                counter.id = counter.id - 1
                attributes_[key] = AttributeData("descriptor", main_Json[key], "-", "Dimensionless",  key, {"pywr_section": key})
                self.attributes.append(ResourceAttr(counter.id, key, "Input"))
                recourseAttributea.append(RecourseAttribute("NETWORK", counter.id, key, attributes_[key], "Dimensionless"))

        self.id=-1
        #self.attributes = []
        self.nodes=[]
        self.links=[]
        self.scenarios=[Scenario()]
        network_attributes[self.name] = attributes_

class Scenario(object):
    """
    Hydra scenario
    """
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
    def __init__(self,  type, value, unit, dimen, name, metadata=None):
        self.hidden="N"
        self.type=type
        self.name = name
        self.unit=unit
        self.dimension=dimen
        if(metadata is None):
            self.metadata="{}"
        else:
            self.metadata =json.dumps(metadata)

        if isinstance(value, basestring) or isinstance(value, float) or isinstance(value, int):
            self.value=value
        else:
            self.value=json.dumps(value)


def get_timeseriesdates(timestepper):
    """
    return timeseries from pywr timestepper
    """
    start=prs(timestepper["start"])
    end=prs(timestepper["end"])
    timestep=(timestepper["timestep"])
    timeseries=[]
    timeseries=[]
    while (end>=start):
        timeseries.append(start)
        start = start + timedelta(days=timestep)
    return timeseries

def get_variable_attribute_type_and_value(value_, name, counter, attributes_, _type, metadata_, res_attributes=None):
    value = value_
    type = "descriptor"
    metadata={}
    for key in metadata_.keys():
        metadata[key]=str(metadata_[key])

    counter.id = counter.id - 1
    attributes_[name] = AttributeData(type, value, "-", "Dimensionless", metadata)
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute("NODE", counter.id, name, attributes_[name], "Dimensionless"))

def get_attribute_type_and_value(value_, name, counter, attributes_, _type, res_attributes=None):
    """
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
    """
    metadata = {}
    global scenarios
    global ref_parameters
    #if type(value_) is dict:
    value =value_
    #else:
    #    value=value_
    attr_type="descriptor"
    counter.id = counter.id - 1
    attributes_[name] = AttributeData(attr_type, value, "-", "Dimensionless", metadata)
    if(res_attributes!=None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute("NODE", counter.id, name , attributes_[name],"Dimensionless"))

def get_new_attributes(attrlist, c_attrlist):
    """
    Get the new attributes which are used in this model to push them to the Hydra
    :param attrlist: Current Hydra attributes list
    :param c_attrlist: Attributes
    :return: new attributes list
    """
    vars_attr = ["received_water", "storage", "flow", "mean_flow"]
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

def get_pywr_json_from_file(filename):
    json_list = []
    f = open(filename, "r")
    json_string = ""
    while 1:
        line = f.readline()
        if not line: break
        json_string += line
    f.close()
    main_json = json.loads(json_string)  # , object_hook=lambda d: namedtuple("X", d.keys(), rename=False, verbose=False)(*d.values()))
    json_list.append(main_json)
    if "includes" in main_json:
        for included_file in main_json["ncludes"]:
            ss = os.path.dirname(included_file)
            if (ss == ""):
                included_file = os.path.join(json_file__folder[0], included_file)
            included_f = open(included_file, "r")
            included_json_string = ""
            while 1:
                line = included_f.readline()
                if not line: break
                included_json_string += line
            included_f.close()

            included_x = json.loads(included_json_string, object_hook=lambda d: namedtuple("X", d.keys())(*d.values()))
            json_list.append(included_x)

            if ("nodes" in included_x):
                for node in included_x["nodes"]:
                    main_json.nodes.append(node)

            if "edges" in included_x:
                for edge in included_x["edges"]:
                    main_json.edges.append(edge)
    return main_json, json_list

def import_nodes(main_json, network, project_attributes, counter):
    '''
      adding pywr nodes to hydra network which includes their attributes
    :param main_json: the json which contains the pywr model
    :param network: hydra networ
    :param counter: an object is used to get ids, which are negative as it is not saved
    :param project_attributes: a dict contains all the network attributes, attribute name is the key, the value is the attribute
    :return nodes_ids: dict contains the nodes ids, name is key, id is the value
    :return nodes_types which is a dict contains node type as key and name as value
    '''
    nodes_ids={}
    nodes_types={}
    nodes_attributes = {}
    global ref_parameters
    for node_ in main_json["nodes"]:
        node = Node(node_, counter, nodes_attributes)
        nodes_ids[node_["name"]] = node.id
        if node.type in nodes_types.keys():
            nodes_types[node.type].append(node.name)
        else:
            nodes_types[node.type] = [node.name]
        network.nodes.append(node)
        counter.id = counter.id - 1
        for attr_name in nodes_attributes[node.name].keys():
            if( attr_name not in project_attributes):
                attr=Attribute(attr_name)
                project_attributes[attr_name]=attr
    return nodes_ids, nodes_types,  nodes_attributes

def import_links(main_json, network, counter, project_attributes, nodes_ids):
    '''
     import pywr edges to hydra network links
    :param main_json: the json which contains the pywr model
    :param network: hydra networ
    :param counter: an object is used to get ids, which are negative as it is not saved
    :param project_attributes: a dict contains all the network attributes, attribute name is the key, the value is the attribute
    :param nodes_ids: dict contains the nodes ids, name is key, id is the value
    :return:links_types which is a dict contains link type as key and name as value
    :return links_attributes which is a dict contains attribute name as key and the attributes as the value
    '''
    links_types={}
    links_attributes={}
    global ref_parameters
    for edge_ in main_json["edges"]:
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
    return  links_types,links_attributes

def set_resource_attribute_id(network, attributes):
    '''
    adjust the recsource scenario attribute id
    :param network: hydra networ
    :param attributes: Hydra attributes which contains the new attributes which are created for this model
    '''
    attrs_names_ids = {}
    for tt in attributes:
        attrs_names_ids[tt.name] = tt.id
    for rs in recourseAttributea:
        try:
            rs.attr_id = attrs_names_ids[rs.attr_id]
        except:
            pass
    network.scenarios[0].resourcescenarios = recourseAttributea
    for rs in network.attributes:
        rs.attr_id = attrs_names_ids[rs.attr_id]
    for node in network.nodes:
        for rs in node.attributes:
            rs.attr_id=attrs_names_ids[rs.attr_id]
    for link in network.links:
        for rs in link.attributes:
            rs.attr_id = attrs_names_ids[rs.attr_id]

def import_net (filename, c_attrlist, connector=None):
    """
    Main function which reads the pywr model JSON  string from a file
    It will extracts the main network elements from it and set them in a form which is recognized by Hydra
    then it will push to hydra database
    :param filename: name of the file  which conatins the Json string
    :param connector: a connector object which is used to communicates with Hydra
    :return: Hydra network, nodes_types, links_types
    """
    # sometimes json string has include statment which include another file whcih contains the
    # another JSON string, if it is the case it will be added to json_list to added them to the model
    json_file__folder.append(os.path.dirname(filename))
    counter=Counter()
    recorders=[]
    domains=[]
    project_attributes = {}
    global ref_parameters
    # dict contains the main pywr variables according to the node type
    nodes_vars_types = {"output": "received_water", "storage": "storage", "link": "flow", "reservoir": "storage", "input":"mean_flow", "catchment":"seasonal_fdc"}
    network_attributes={}
    main_json, json_list=get_pywr_json_from_file(filename)
    global timeseries
    if "timestepper" in main_json:
        timeseries=get_timeseriesdates(main_json["timestepper"])
    global scenarios
    if "scenarios" in main_json:
        for scenario in main_json["scenarios"]:
            scenarios[scenario["name"]]={"name": scenario["name"], "size": scenario["size"]}

    timestepper=None
    if ("timestepper" in main_json):
        timestepper = main_json["timestepper"]

    solver_name=None
    if ("solver" in main_json):
        solver_name=main_json["solver"]

    if ("metadata" in main_json):
        network = Network("Pywr",solver_name, -1, counter, domains, recorders, network_attributes,
                          timestepper, main_json)
    else:
        network = Network("Pywr", solver_name, -1, counter, domains, recorders, network_attributes,
                          timestepper)
    #add new new network attributes to project attributes
    for attr_name in network_attributes[network.name].keys():
        if (attr_name not in project_attributes):
            attr = Attribute(attr_name)
            project_attributes[attr_name] = attr
    counter.id=counter.id-1
    i = 0
    for json_ in json_list:
        if ("parameters" in json_):
            for i in range(0, len(json_["parameters"].keys())):
                k = json_["parameters"].keys()[i]
                counter.id = counter.id - 1
                get_attribute_type_and_value(json_["parameters"].values()[i], k, counter, ref_parameters, "default")

    ## adding pywr nodes to hydra network which includes their attributes
    nodes_ids, nodes_types, nodes_attributes = import_nodes(main_json, network, project_attributes, counter)
    ## add pywr edges to hydra network links
    links_types, links_attributes = import_links(main_json, network, counter, project_attributes, nodes_ids)
    new_attr = get_new_attributes(project_attributes.values(), c_attrlist)
    if connector!=None:
        attributes = connector.call("add_attributes", {"attrs": new_attr})
    else:
        attributes=c_attrlist

    set_resource_attribute_id(network, attributes)

    return network,nodes_types, links_types

def add_network(network, connector,nodes_types, links_types):
    #Push the network to Hydra
    project=Project()
    proj = connector.call("add_project", {"project": project.__dict__})
    network.project_id=proj.id
    #print json.dumps(get_dict(network))
    NetworkSummary = connector.call("add_network", {"net": get_dict(network)})
    # Push pywr template to Hydra and set nodes and links types
    set_resource_types(nodes_types, links_types, network.type, NetworkSummary, connector)
    return NetworkSummary

def set_resource_types(nodes_types, links_types, networktype, NetworkSummary, connection):
    ## assign resources types using the template file
    template_file="..\PywrNetwork.xml"
    #print("Setting resource types based on %s." % template_file)
    with open(template_file) as f:
        xml_template = f.read()
    template = connection.call("upload_template_xml", {"template_xml":xml_template})
    type_ids = {}
    warnings = []
    for tmpltype in template.get("types", []):
        if tmpltype["name"].lower() == (str(networktype).lower()):
            type_ids.update({tmpltype["name"]: tmpltype["id"]})
            break

    for type_name in nodes_types:
        for tmpltype in template.get("types", []):
            if tmpltype["name"] == (str(type_name).lower()):
                type_ids.update({tmpltype["name"]: tmpltype["id"]})
                break

    for type_name in links_types:
        for tmpltype in template.get("types", []):
            if tmpltype["name"] == type_name.lower():
                type_ids.update({tmpltype["name"]: tmpltype["id"]})
                break
    for tmpltype in template.get("types", []):
        if tmpltype["name"] == networktype.lower():
            type_ids.update({tmpltype["name"]: tmpltype["id"]})
            break
    args = []

    if networktype == "":
        warnings.append("No network type specified")
    elif type_ids.get(networktype):
        args.append(dict(
            ref_key = "NETWORK",
            ref_id  = NetworkSummary["id"],
            type_id = type_ids[networktype],
        ))
    if NetworkSummary.get("nodes", []):
        for node in NetworkSummary["nodes"]:
            for typename, node_name_list in nodes_types.items():
                type_id=is_in_dict(typename, type_ids)
                if type_id !=None  and node.name in node_name_list:
                    args.append(dict(
                        ref_key = "NODE",
                        ref_id  = node.id,
                        type_id = type_id,
                    ))

    if NetworkSummary.get("links", []):
        for link in NetworkSummary["links"]:
            for typename, link_name_list in links_types.items():
                type_id = is_in_dict(typename, type_ids)
                if type_id !=None and link["name"] in link_name_list:
                    args.append(dict(
                        ref_key = "LINK",
                        ref_id  = link["id"],
                        type_id = type_id ,
                    ))

    connection.call("assign_types_to_resources", {"resource_types":args})