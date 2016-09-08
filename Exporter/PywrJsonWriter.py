import json
import jsonpickle
import random
from collections import namedtuple
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import datetime
import inspect
import os

nodes_parameters = {}

parameters = {}

class Recorderthreshold (object):
    def __init__(self, attr, res, metadata, single_parameters):
        to_be_deleted=[]
        self.type = 'recorderthreshold'
        self.type=metadata['type']
        self.recorder=metadata['recorder']
        self.predicate=metadata['predicate']
        self.threshold=float(metadata['threshold'])
        self.values=json.loads(res.value.value)


class Aggregated (object):
    def __init__(self, attr, res, metadata, single_parameters):
        to_be_deleted=[]
        self.type = 'aggregated'
        self.agg_func=metadata['agg_func']
        self.parameters=json.loads(res.value.value)
        for item in self.parameters:
            if(item in single_parameters.keys()):
                to_be_deleted.append(item)

        for item in to_be_deleted:
            parameters[item]=single_parameters[item]
            del single_parameters[item]


class Controlcurveindex(object):
    def __init__(self,attr, res, metadata, single_parameters):
        to_be_deleted = []
        self.type="controlcurveindex"
        self.comment = metadata['comment']
        self.storage_node = metadata['storage_node']
        self.control_curves=json.loads(res.value.value)

        for item in self.control_curves:
            if (item in single_parameters.keys()):
                to_be_deleted.append(item)

        for item in to_be_deleted:
            parameters[item] = single_parameters[item]
            del single_parameters[item]


class Indexedarray(object):
    def __init__(self, attr, res, metadata, single_parameters):
        to_be_deleted = []
        self.type = 'indexedarray'
        self.comment = metadata['comment']
        self.index_parameter = metadata['index_parameter']
        self.params=json.loads(res.value.value)
        if (self.index_parameter in single_parameters.keys()):
            to_be_deleted.append(self.index_parameter)

        for item in self.params:
            if (item in single_parameters.keys()):
                to_be_deleted.append(item)

        for item in to_be_deleted:
            parameters[item] = single_parameters[item]
            del single_parameters[item]

class Monthlyprofilecontrolcurve (object):
    def __init__(self, attr, res, metadata, single_parameters):
        to_be_deleted = []
        self.type='monthlyprofilecontrolcurve'
        self.storage_node = metadata['storage_node']
        self.scale = float(metadata['scale'])
        pars=json.loads(res.value.value)
        self.profile =pars[0]
        self.control_curve=pars[1]
        self.values=pars[2]

        if (self.profile in single_parameters.keys()):
            to_be_deleted.append(self.profile)
            self.profile=single_parameters[self.profile]

        if (self.control_curve in single_parameters.keys()):
            to_be_deleted.append(self.control_curve)
            self.control_curve = single_parameters[self.control_curve]


        if (self.values in single_parameters.keys()):
            to_be_deleted.append(self.values )
            self.values = single_parameters[self.values]

        for item in to_be_deleted:
            #parameters[item] = single_parameters[item]
            del single_parameters[item]


class ControlCurveInterpolated (object):
    def __init__(self, attr, res, metadata, single_parameters):
        self.type='ControlCurveInterpolated'
        self.control_curve =metadata['control_curve']
        self.values=json.loads(res.value.value)
        self.storage_node=metadata['storage_node']
        if (self.control_curve in single_parameters.keys()):
            parameters[self.control_curve] = single_parameters[self.control_curve]
            del single_parameters[self.control_curve]



def adjuest_parameters(complex_attrinbtes):
    to_be_deleted=[]
    for attr_name in complex_attrinbtes.keys():
        attr=complex_attrinbtes[attr_name]

        if (attr.type == 'controlcurveindex'):
            for item in attr.control_curves:
                if (item in complex_attrinbtes.keys()):
                    to_be_deleted.append(item)
        elif (attr.type == 'indexedarray'):
            if (attr.index_parameter in complex_attrinbtes.keys()):
                to_be_deleted.append(attr.index_parameter)

            for item in attr.params:
                if (item in complex_attrinbtes.keys()):
                    to_be_deleted.append(item)

        elif (attr.type == 'aggregated'):
            for item in attr.parameters:
                if (item in complex_attrinbtes.keys()):
                    to_be_deleted.append(item)

        elif (attr.type == 'ControlCurveInterpolated'):
            if (attr.control_curve in complex_attrinbtes.keys()):
                to_be_deleted.add(attr.control_curve)
        elif (attr.type == 'monthlyprofilecontrolcurve'):
            if (attr.profile in complex_attrinbtes.keys()):
                to_be_deleted.append(attr.profile)

            if (attr.control_curve in complex_attrinbtes.keys()):
                to_be_deleted.append(attr.control_curve)

            if (attr.values in complex_attrinbtes.keys()):
                to_be_deleted.append(attr.values)
        elif (attr.type == 'recorderthreshold'):
            pass

    for item in to_be_deleted:
        parameters[item] = complex_attrinbtes[item]
        del complex_attrinbtes[item]


class Node(dict):
    def __init__(self, node_, attributes_ids, resourcescenarios_ids):
        self.name=node_.name
        single_parameters={}
        attributes={}
        aggregated_attributes=[]
        geographic=None
        for attr_ in node_.attributes:
            attr=attributes_ids[attr_.attr_id]
            res=resourcescenarios_ids[attr_.id]
            metadata = json.loads(res.value.metadata)
            if(metadata ['single']=='no'):
                aggregated_attributes.append(attr_)
                continue
            if(attr.name=='node_type'):
                self.type=res.value.value
            elif attr.name == 'geographic':
                geographic = json.loads(res.value.value)

            elif res.value.type == 'descriptor':
                single_parameters[attr.name] = res.value.value

            elif res.value.type == 'scalar' and metadata['single'] == 'yes':
                single_parameters[attr.name] =float(res.value.value)

            elif res.value.type == 'timeseries' and metadata['single']== 'yes':
                if 'column' in metadata.keys():
                    single_parameters[attr.name] = get_timesreies_values(res.value.value, metadata['column'],
                                                                         json.loads(res.value.metadata))
                else:
                    single_parameters[attr.name] = get_timesreies_values(res.value.value, attr.name,
                                                                         json.loads(res.value.metadata))

            elif res.value.type == 'array' and metadata['single'] == 'yes':
                single_parameters[attr.name] =json.loads (res.value.value)

        complex_attributes={}
        for attr_ in aggregated_attributes:
            attr = attributes_ids[attr_.attr_id]
            res = resourcescenarios_ids[attr_.id]
            metadata = json.loads(res.value.metadata)
            if(metadata['type']=='controlcurveindex'):
                complex_attributes[attr.name] =Controlcurveindex(attr, res, metadata, single_parameters)

            elif (metadata['type']=='indexedarray'):
                complex_attributes[attr.name] =Indexedarray(attr, res, metadata, single_parameters)

            elif (metadata['type'] == 'aggregated'):
                complex_attributes[attr.name] =Aggregated(attr, res, metadata, single_parameters)

            elif (metadata['type'] == 'ControlCurveInterpolated'):
                complex_attributes[attr.name] =ControlCurveInterpolated(attr, res, metadata, single_parameters)

            elif (metadata['type'] == 'monthlyprofilecontrolcurve'):
                complex_attributes[attr.name] =Monthlyprofilecontrolcurve(attr, res, metadata, single_parameters)

            elif (metadata['type'] == 'recorderthreshold'):
                complex_attributes[attr.name] = Recorderthreshold(attr, res, metadata, single_parameters)


        adjuest_parameters(complex_attributes)

        for item in complex_attributes.keys():
            self.__dict__[attr.name] =complex_attributes[item]
            attributes[attr.name] = self.__dict__[attr.name]


        for item in single_parameters.keys():
            self.__dict__[item] = single_parameters[item]
            attributes[item] = self.__dict__[item]

        self.position = {}

        self.position['schematic'] = [node_.x, node_.y]
        if(geographic==None):
            self.position['geographic'] = []
        else:
            self.position['geographic'] = geographic
        nodes_parameters[self.name]=attributes

def get_timesreies_values(value, column, metadata):

    if('type' in metadata.keys()):
        type_ = metadata['type']
    else:
        type_ = 'default'

    values={}
    vv = json.loads(value)
    contents=[]
    if(type_ == 'dailyprofile'):
        contents.append('Index,' + column + '\n')
    elif (type_ == 'dataframe'):
        contents.append('Timestamp,' + 'Data' + '\n')

    else:
        contents.append('Date,'+column+'\n')
    day=1
    for key in vv.keys():
        for date in sorted(vv[key].keys()):
            if(date.startswith('9999') ):
                if(type_ == "monthlyprofile"):
                    values['type'] = type_
                    values['values'] = ger_arrayvalues(vv[key])
                    return values
                elif type_ == "dailyprofile":
                    contents.append(str(day) + ',' + str(vv[key][date]) + '\n')
                    day+=1
            else:
                contents.append(date+','+str(vv[key][date])+'\n')
    # in case of  dailyprofile, hydra save only 365 dats in a year while
    # pywr required values for 356 days, so days 365 is repeated till fix that in hydar
    if(type_ == "dailyprofile"):
        contents.append(str(day) + ',' + str(vv[key][date]))
    if ('parse_dates' in metadata.keys()):
        if(metadata['parse_dates'].lower() == 'true'):
            values['parse_dates'] = True
        else:
            values['parse_dates']=False

    if ('index_col' in metadata.keys()):
        values['index_col'] = int(metadata['index_col'])

    if ('dayfirst' in metadata.keys()):
        values['dayfirst'] = metadata['dayfirst']


    values['type'] = type_
    values['url']=contents
    if(type_!='dataframe'):
        values['column']=column


    return values

def write_time_series_tofile(contents, filename):
    print "File name ======>", filename
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
    recorders={}
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        res = resourcescenarios_ids[attr_.id]
        if (attr.name == 'recorders'):
            values=json.loads(res.value.value)
            metadata = json.loads(res.value.metadata)
            for value in values:
                dic={}
                recorders[value]=dic
                for key in metadata.keys():
                    if key.startswith(value+'@'):
                        item=key.replace(value+'@','')
                        if item =='timesteps':
                            dic[item] = int(metadata[key])
                        else:
                            dic[item]=metadata[key]


    return recorders

class Domain(object):
    def __init__(self, name, color):
        self.name = name
        self.color = color

class Solver(object):
    def __init__(self, network, attributes_ids, resourcescenarios_ids):
        for attr_ in network.attributes:
            attr = attributes_ids[attr_.attr_id]
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
                self.timestep = int(res.value.value)

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
    def __init__(self, metadata, timestepper, solver, nodes, edges, domains, parameters,  recorders):
        self.metadata=metadata
        self.timestepper=timestepper
        self.solver=solver
        self.nodes=nodes
        self.edges=edges
        self.domains=domains
        self.parameters=parameters
        self.recorders=recorders

    def get_json(self):
        json_string='{\n\"metadata\": '+json.dumps(get_dict(self.metadata), indent=4)+',\n'
        if len(get_dict(self.timestepper)) > 0:
            json_string=json_string+'\"timestepper\": '+json.dumps(get_dict(self.timestepper), indent=4)+',\n'
        json_string = json_string + '\"nodes\": '+json.dumps(get_dict(self.nodes), indent=4)+',\n'
        json_string = json_string + '\"edges\": ' + json.dumps(get_dict(self.edges), indent=4) + ',\n'
        if(len(self.domains)>0):
            json_string = json_string + '\"domains\": ' + json.dumps(get_dict(self.domains), indent=4) + ',\n'
        json_string = json_string + '\"parameters\": ' +json.dumps(get_dict((parameters)), default=lambda o: o.__dict__, sort_keys=True, indent=4)

        if len(self.recorders)>0:
            json_string = json_string + ',\n\"recorders\": ' + json.dumps(get_dict(self.recorders), indent=4)+'\n}'
        else:
            json_string = json_string + '\n}'

        return  json_string


def get_parameters_refs(nodes):
    for i in range(0, len(nodes)):
        node = nodes[i]
        for j in range(i+1, len(nodes)):
            node_=nodes[j]
            attrs=nodes_parameters[node.name]
            attrs_ = nodes_parameters[node_.name]
            for attr in attrs.keys():
                for attr_ in attrs_.keys():
                    if(attr==attr_):
                        if hasattr(attrs[attr], "type") and hasattr(attrs_[attr_],'type'):
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
    json_file__folder=os.path.dirname(output_file)
    nodes=[]
    edges=[]
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
        res = resourcescenarios_ids[attr_.id]
        if (attr.name == 'domains'):
            val = json.loads(res.value.value)
            if(len(val)==2):
                name = val[0][0]
                color = val[0][1]
                domains.append(Domain(name, color))
    solver = Solver(network, attributes_ids, resourcescenarios_ids)
    nodes_id_name={}
    for node_ in network.nodes:
        node=Node(node_, attributes_ids, resourcescenarios_ids)
        nodes_id_name[node_.id]=node_.name
        nodes.append(node)
    parameters=get_parameters_refs(nodes,)

    for link_ in network.links:
        edge=Edge(link_, attributes_ids, resourcescenarios_ids, nodes_id_name)
        edges.append(edge.attrs)
    for node in nodes:
        for i in range(0, len(node.__dict__.keys())):
            k = node.__dict__.keys()[i]
            if (k.lower() != 'name' and k.lower() != 'type' and k.lower() != 'position'):
                value=node.__dict__.values()[i]
                if type(value) is dict:
                    if value['type'] == 'arrayindexed' or value['type'] == 'dailyprofile' or value['type'] == 'dataframe':
                        file_name=node.name+"_"+k+'.csv'
                        write_time_series_tofile(value['url'], os.path.join(json_file__folder, file_name))
                        value['url']=file_name
    for k in parameters:
        value=parameters[k]

        if type(value) is dict and 'type' in value.keys():
            if value['type'] == 'arrayindexed' or value['type'] == 'dailyprofile':
                file_name = node.name + "_" + k + '.csv'
                write_time_series_tofile(value['url'], os.path.join(json_file__folder, file_name))
                value['url'] = file_name


    pywrNetwork=PywrNetwork(metadata, timestepper, solver, nodes, edges, domains, parameters,  recorders)
    with open(output_file, "w") as text_file:
        text_file.write(pywrNetwork.get_json())

        #text_file.write(json.dumps(get_dict(pywrNetwork), indent=4))






