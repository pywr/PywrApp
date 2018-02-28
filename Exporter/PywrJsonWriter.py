import json
from dateutil import parser
import os

from HydraLib.PluginLib import write_progress

from Lib.utilities import get_dict

nodes_parameters = {}

parameters = {}
recorders={}


from Lib.data_files_writer import write_tablesarray_to_hdf

has_tablerecorder=False

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
    '''
    Pywr node and its attributes
    '''
    def __init__(self, node_, attributes_ids, resourcescenarios_ids):
        '''
        :param node_: Hydra node
        :param attributes_ids:  dict which its keys are attributes ids and values are the atrributes
        :param resourcescenarios_ids: dict, its keys are resourcescenarios ids and values are the resourcescenarios
        '''
        global has_tablerecorder
        self.name=node_.name
        single_parameters={}
        attributes={}
        aggregated_attributes=[]
        geographic=None
        for attr_ in node_.attributes:
            if attr_.attr_is_var == 'Y':
                attr = attributes_ids[attr_.attr_id]
                if has_tablerecorder == True and attr.name=='mean_flow':
                    continue
                if attr_.id not in resourcescenarios_ids.keys():
                    continue
                res = resourcescenarios_ids[attr_.id]
                metadata = json.loads(res.value.metadata)
                if 'type' in metadata:
                    dic={}
                    recorders[attr.name] =dic
                    dic["node"]=node_.name
                    for key in metadata.keys():
                        if key=="user_id":
                            continue
                        if key == 'timesteps':
                            dic[key] = int(metadata[key])
                        elif key == 'percentiles'or key == 'months' or key =='coefficients':
                            dic[key] = json.loads(metadata[key])
                        elif key== 'epsilon':
                            dic[key] = float(metadata[key])
                        elif key== 'is_constraint':
                            dic[key]=bool(metadata[key])
                        else:
                            dic[key] = metadata[key]
                continue
            attr=attributes_ids[attr_.attr_id]
            if attr_.id not in resourcescenarios_ids.keys():
                continue
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
                if 'type' in metadata.keys():
                    vals={}
                    vals['type']='constant'
                    vals['value']=float(res.value.value)
                    #adding additional value related data from hydra metadat
                    is_ref=False
                    for key in metadata:
                        if key != 'type' and key != 'user_id' and key != 'single'and key!='value':
                            if key=='is_variable':
                                vals[key] = bool(metadata[key])
                            elif key=='upper_bounds' or key =='lower_bounds':
                                vals[key]=float(metadata[key])
                            elif key=='ref_name':
                                is_ref=True
                                ref_name=metadata[key]
                            else:
                                vals[key] = metadata[key]
                    if is_ref==False:
                        single_parameters[attr.name] =vals#float(res.value.value)
                    else:
                        single_parameters[attr.name] = ref_name
                        parameters[ref_name]=vals
                else:
                    single_parameters[attr.name] = float(res.value.value)
            elif res.value.type == 'timeseries' and metadata['single']== 'yes':
                if 'column' in metadata.keys():
                    single_parameters[attr.name] = get_timesreies_values(res.value.value, metadata['column'],
                                                                         json.loads(res.value.metadata))
                else:
                    single_parameters[attr.name] = get_timesreies_values(res.value.value, attr.name,
                                                                         json.loads(res.value.metadata))

            elif res.value.type == 'array' and metadata['single'] == 'yes':
                if 'type' in metadata.keys() and metadata['type']=='tablesarray':
                    single_parameters[attr.name] = get_tables_array_values(res.value.value, metadata)
                elif 'type' in metadata.keys() and metadata['type']=='constantscenario':
                    single_parameters[attr.name] = get_constantscenario_values(res.value.value, metadata)
                elif 'type' in metadata.keys() and metadata['type'] == 'annualharmonicseries':
                    single_parameters[attr.name] = get_annualharmonicseries_values(res.value.value, metadata)
                elif res.value.type == 'array' and metadata['single'] == 'yes':
                    single_parameters[attr.name] =json.loads (res.value.value)

        complex_attributes={}
        '''
        get pywr node complex attributes 
        it checks the metadata which specifies the attribute type, and accordingly construct it 
        '''
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
        if hasattr(node_, "x") and hasattr(node_, "y"):
            self.position = {}
            self.position['schematic'] = [node_.x, node_.y]
            if(geographic==None):
                self.position['geographic'] = []
            else:
                self.position['geographic'] = geographic
        nodes_parameters[self.name]=attributes

def get_tables_array_values(value, metadata):
    values={}
    for key in metadata.keys():
        if key =='user_id' or key =='single':
            continue
        values[key]=metadata[key]
    values['url'] = value
    return values

def get_constantscenario_values (value, metadata):
    values={}
    for key in metadata.keys():
        if key =='user_id' or key =='single':
            continue
        values[key]=metadata[key]
    values['values'] = json.loads(value)
    return values

def get_annualharmonicseries_values(value, metadata):
    value=json.loads(value)
    values = {}
    values['type']=metadata['type']
    values['mean'] = value['mean']
    values['amplitudes'] = value['amplitudes']
    values['phases'] = value['phases']
    return values

def get_scenarios(network, attributes_ids, resourcescenarios_ids):
    scenarios=[]
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        res = resourcescenarios_ids[attr_.id]
        metadata=json.loads(res.value.metadata)
        if "type" in metadata and metadata['type']=='scenario':
            metadata['size']
            scenarios.append({'name':attr.name, 'size':int(metadata['size'])})
    return scenarios
def get_timesreies_values(value, column, metadata):
    '''
    :param value: Json string which represnt the values
    :param column: the column name
    :param metadata: dict contains the attribute metadata which are required to write the values
    :return: dict which contains
    '''
    if('type' in metadata.keys()):
        type_ = metadata['type']
    else:
        type_ = 'default'
    values={}
    vv = json.loads(value)
    header=[]
    contents=[]
    header = []

    contents = []
    if (type_ == 'dailyprofile'):
        header.append('Index')
    elif (type_ == 'dataframe'):
        header.append('Timestamp')
    else:
        header.append('Date')
    day = 1

    if 'checksum' in metadata.keys():
        pass  # values['checksum']=metadata['checksum']

    if 'scenario' in metadata.keys():
        values['scenario'] = metadata['scenario']
        all_scenarios_data={}
        int_keys={}
        for item in vv['0'].keys():
            int_keys[int(item)]=item
        for sec_ in sorted(int_keys.keys()):
            #if (type_ == 'dataframe'):
            header.append(int_keys[sec_])
            vv_=vv['0'][int_keys[sec_]]
            dated_dict = {}
            for ts in (vv_.keys()):
                dated_dict[parser.parse(ts)] = ts
            ll = sorted(dated_dict.keys())
            for date_ in ll:
                if date_ in all_scenarios_data:
                    scenarios_date_data=all_scenarios_data[date_]
                else:
                    scenarios_date_data=[]
                    all_scenarios_data[date_]=scenarios_date_data
                scenarios_date_data.append(vv_[dated_dict[date_]])
        contents=','.join(header)
        for date in sorted(all_scenarios_data.keys()):
            contents+='\n'+str(date)+','+','.join(all_scenarios_data[date])
    else:
        if (type_ == 'dataframe'):
            contents.append(header[0]+',Data' + '\n')
        else:
            contents.append(header[0] +','+column + '\n')
        for key in vv.keys():
            import operator
            print "SSS", vv[key].keys()
           #ss=vv[key].keys().sort(key=operator.itemgetter('date'))

            dated_dict={}
            for ts in (vv[key].keys()):
                print "TS:",(ts)
                dated_dict[parser.parse(ts)]=ts
            ll=(dated_dict.keys())
            ll.sort(key = lambda d: (d.year, d.month, d.day))
            print ll
            for date_ in ll:
                line=''
                date=dated_dict[date_]
                if(date.startswith('9999') ):
                    if(type_ == "monthlyprofile"):
                        values['type'] = type_
                        values['values'] = get_array_values(vv[key])
                        return values
                    elif type_ == "dailyprofile":
                        contents.append(str(day) + ',' + str(vv[key][date]) + '\n')
                        day+=1
                else:
                    contents.append(date+','+str(vv[key][date])+'\n')
        # in case of  dailyprofile, hydra save only 365 days in a year while
        # pywr required values for 356 days, so days 365 is repeated till fix that in hydar
        # this should be modified shortly and uses the Hydra hashtable attribute type
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
        values['dayfirst'] = bool(metadata['dayfirst'])


    values['type'] = type_
    values['url']=contents
    if(type_!='dataframe'):
        values['column']=column
    else:
        if 'dayfirst' not in values:
            values['dayfirst']=True
        if 'parse_dates' not in values:
            values['parse_dates']=True

    return values

def write_time_series_to_file(contents, filename):
    file = open(filename, "w")
    file.write("".join(contents))
    file.close()

def get_array_values(value_):
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
    def __init__(self,from_node,to_node):
        self.attrs = [from_node,to_node]

    @classmethod
    def from_link(cls, link_, attributes_ids, resourcescenarios_ids, nodes_id_name):
        ss=cls(nodes_id_name[link_.node_1_id], nodes_id_name[link_.node_2_id])
        for attr_ in link_.attributes:
            attr = attributes_ids[attr_.attr_id]
            res = resourcescenarios_ids[attr_.id]
            slot_from=None
            slot_to = None
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
            ss.attrs.append(slot_from)
            ss.attrs.append(slot_to)
        return ss

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

def get_pywr_section(network, attributes_ids, resourcescenarios_ids, pywr_section):
    '''
    :param network: Hydra network
    :param attributes_ids: dict the keys is attributes ids and values are the attributes objects
    :param resourcescenarios_ids: dict the keys is resourcescenarios ids and values are the resourcescenarios objects
    it changes global  has_tablerecorder Boolean
    '''
    recorders={}
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        res = resourcescenarios_ids[attr_.id]
        metadata = json.loads(res.value.metadata)
        if 'pywr_section' in metadata and metadata['pywr_section']==pywr_section:
        #if (attr.name == 'recorder'):
            value=json.loads(res.value.value)
            recorders[attr.name]=get_dict(res)

    return recorders



class Domain(object):
    def __init__(self, name, color):
        self.name = name
        self.color = color

class Solver(object):
    '''
    Solver attribures
    '''
    def __init__(self, network, attributes_ids, resourcescenarios_ids):
        for attr_ in network.attributes:
            attr = attributes_ids[attr_.attr_id]
            res = resourcescenarios_ids[attr_.id]
            if (attr.name == 'solver'):
                self.name=res.value.value

class Timestepper(object):
    '''
     Class contains the data for the model time axis
    '''
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
    '''
    contains the pywr model metadata
    '''
    def __init__(self, network, resourcescenarios_ids, attributes_ids):
       self.title=network.name
       for attr_ in network.attributes:
           attr     = attributes_ids[attr_.attr_id]
           res = resourcescenarios_ids[attr_.id]
           if (attr.name == 'author'):
               self.author = res.value.value
           if (attr.name == 'minimum_version'):
               self.minimum_version = res.value.value
       if hasattr(network, "description"):
           self.description=network.description


class PywrNetwork (object):
    '''
        Class contains all pywr json sections
        The sections are lists contains objects of different pywr model sections
        It is objects with their attaributes
        It will be converted to Json string using json.dumps method latter
    '''
    def __init__(self, metadata, timestepper, solver, nodes, edges, domains, parameters,  recorders, scenarios):
        self.metadata=metadata
        self.timestepper=timestepper
        self.solver=solver
        self.nodes=nodes
        self.edges=edges
        self.domains=domains
        self.parameters=parameters
        self.recorders=recorders
        self.scenarios=scenarios

    def get_json(self):
        '''
        prepare and return the pywr json string from this class varaibles
        '''
        json_string='{\n\"metadata\": '+json.dumps(get_dict(self.metadata), indent=4)+',\n'
        if len(get_dict(self.timestepper)) > 0:
            json_string=json_string+'\"timestepper\": '+json.dumps(get_dict(self.timestepper), indent=4)+',\n'
        if len(self.scenarios)>0:
            json_string +='\"scenarios\" :'+ json.dumps(self.scenarios, indent=4)+',\n'

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
    '''
    :param nodes: the network nodes
    It goes throw their attributes and get parameters references list
    :return: parameters references list
    '''
    for i in range(0, len(nodes)):
        node = nodes[i]
        for j in range(i+1, len(nodes)):
            node_=nodes[j]
            attrs=nodes_parameters[node.name]
            attrs_ = nodes_parameters[node_.name]
            for attr in attrs.keys():
                if attr=='factors':
                    continue
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
                                    if attrs[attr]['values'] == attrs_[attr_]['values']:
                                        if (not attr+'_ref' in parameters):
                                            parameters[attr]={'type': attrs[attr] ['type'], 'values':attrs_[attr_]['values']}
                        else:
                            if attrs[attr] == attrs_[attr_] and attr_ != 'factors':
                                if(not attr+'_ref' in parameters ):
                                    parameters[attr+'_ref' ]= {"type": "constant","values": attrs[attr]}

    for node in nodes:
        for key in node.__dict__.keys():
            for attr in parameters.keys():
                if attr.lower() == (key+'_ref').lower():
                    if parameters[attr]== node.__dict__[key]:
                        node.__dict__[key]=attr
    return  parameters


def pywrwriter(network, attrlist, output_file, steps):
    '''
    It reads the Hydra network and convert it to a equivalent pywr model
    :param network: Hydra network
    :param attrlist: projects hydra attibutes
    :param output_file: the json file name
    :param steps: integer which is used to indicates the script progress
    '''
    json_file__folder=os.path.dirname(output_file)
    write_progress(4, steps)
    nodes=[]
    edges=[]
    domains=[]
    attributes_ids={}
    for attr in attrlist:
        attributes_ids[attr.id]=attr
    resourcescenarios_ids=get_resourcescenarios_ids(network.scenarios[0].resourcescenarios)
    timestepper=Timestepper(network, attributes_ids, resourcescenarios_ids)
    metadata = Metadata(network, resourcescenarios_ids, attributes_ids)
    get_recotds(network, attributes_ids, resourcescenarios_ids)
    scenarios=get_scenarios(network, attributes_ids, resourcescenarios_ids)
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
    #To do check parameters refs for the new pywr format
    parameters=get_parameters_refs(nodes)
    to_be_added_to_nodes=[]
    for link_ in network.links:
        #test if the link type is link or river, if so it will be added to the pywr nodes
        if link_.types[0].name.lower() =='link':# or link_.types[0].name=='river':
            node = Node(link_, attributes_ids, resourcescenarios_ids)
            nodes_id_name[node_.id] = node_.name
            nodes.append(node)
            to_be_added_to_nodes.append(link_)
        else:
            edge=Edge.from_link(link_, attributes_ids, resourcescenarios_ids, nodes_id_name)
            edges.append(edge.attrs)
    #add edges for links nodes
    for link_ in to_be_added_to_nodes:
        edges.append(Edge(nodes_id_name[link_.node_1_id],link_.name).attrs)
        edges.append(Edge(link_.name,nodes_id_name[link_.node_2_id]).attrs)

    for node in nodes:
        for i in range(0, len(node.__dict__.keys())):
            k = node.__dict__.keys()[i]
            if (k.lower() != 'name' and k.lower() != 'type' and k.lower() != 'position'):
                value=node.__dict__.values()[i]
                if type(value) is dict:
                    try:
                        if value['type'] == 'arrayindexed' or value['type'] == 'dailyprofile' or value['type'] == 'dataframe':
                            file_name=node.name+"_"+k+'.csv'
                            write_time_series_to_file(value['url'], os.path.join(json_file__folder, file_name))
                            value['url']=file_name
                        else:
                            if value['type'] == 'tablesarray':
                                file_name = node.name + "_" + k + '.h5'
                                write_tablesarray_to_hdf(os.path.join(json_file__folder, file_name), value['where'],
                                                         value['node'], json.loads(value['url']))
                                value['url'] = file_name
                    except:
                        pass
    for k in parameters:
        value=parameters[k]
        if type(value) is dict and 'type' in value.keys():
            if value['type'] == 'arrayindexed' or value['type'] == 'dailyprofile':
                file_name = node.name + "_" + k + '.csv'
                write_time_series_to_file(value['url'], os.path.join(json_file__folder, file_name))
                value['url'] = file_name
            else:
                if value['type'] == 'tablesarray':
                     file_name=node.name+"_"+k+'.h5'
                     write_tablesarray_to_hdf(os.path.join(json_file__folder, file_name), value['where'], value['node'], json.loads(value['url']))
                     value['url'] = file_name
    write_progress(5, steps)

    pywrNetwork=PywrNetwork(metadata, timestepper, solver, nodes, edges, domains, parameters,  recorders, scenarios)
    write_progress(6, steps)
    pywr_json_string=pywrNetwork.get_json()
    with open(output_file, "w") as text_file:
        text_file.write(pywr_json_string)
        #text_file.write(json.dumps(get_dict(pywrNetwork), indent=4))
    return pywr_json_string






