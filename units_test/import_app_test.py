import unittest
from collections import namedtuple
import json
from Lib.utilities import get_dict, is_in_dict
from Exporter.PywrJsonWriter import get_pywr_section, get_resourcescenarios_ids
from HydraLib.PluginLib import JSONPlugin
from Importer.PywrJsonReader import  get_pywr_json_from_file, import_net



def compare_nodes_attributes(excpected_section, imported_section):
    '''
    Compare between the expected hydra section with the acutal imported section
    :param excpected_section: the expected section
    :param imported_section: the actual imported section
    :return:
    '''
    #print imported_section.values()
#    assert len(imported_section) == len(excpected_section)
    for resourceAttribute in excpected_section:
        asserted=False
        for node in imported_section.keys():
            for attr in node.attributes:
                #check to get the specified node
                if attr.id== resourceAttribute['resource_attr_id']:
                    for imported_res in imported_section[node]:
                        #check for the required node attribute
                        if imported_res['attr_id']==resourceAttribute['attr_id']:
                            print "Check attributes:",resourceAttribute['value']['name'], " for node:",node.name
                            #check all the reseource scnario values
                            for key in imported_res:
                                assert key in resourceAttribute
                                assert imported_res[key] ==resourceAttribute[key]
                            asserted=True
        #check if the scenario attribute is found and tested ....
        assert asserted==True


def compare_hydra_section(excpected_section, imported_section):
    '''
    Compare between the expected hydra section with the acutal imported section
    :param excpected_section: the expected section
    :param imported_section: the actual imported section
    :return:
    '''
#    assert len(imported_section) == len(excpected_section)
    for record in excpected_section:
        # firsttest is the recorder is added
        record_name = record['value']['name']
        print "Testing: ", record_name
        assert record_name in imported_section
        for key_ in record:
            print "Testing ", key_
            assert key_ in get_dict(imported_section[record_name]);
            "attribute was not added!"
            assert record[key_] == imported_section[record_name][key_];
            "attribute value is not correct!"

class Pywr_to_hydra_importer(unittest.TestCase):
    def setUp(self):
        attributes_json_file=r"attributes.json"

        f = open(attributes_json_file, 'r')
        attributes_json_string=''
        while 1:
            line = f.readline()
            if not line: break
            attributes_json_string += line
        f.close()
        hydra_attributes = json.loads(attributes_json_string, object_hook=lambda d: namedtuple('X', d.keys(), rename=False, verbose=False)(*d.values()))
        filename = r"demand_saving2_with_variables.json"
        self.pywr_model, json_list = get_pywr_json_from_file(filename)
        hydra_network, nodes_types, links_types = import_net(filename, hydra_attributes)
        self.hydra_model = (hydra_network)
        self.resourcescenarios_ids=get_resourcescenarios_ids(hydra_network.scenarios[0].resourcescenarios)
        self.attributes_ids = {}
        for attr in hydra_attributes:
            self.attributes_ids[attr.id] = attr

        self.nodes_id_name = {}
        for node_ in hydra_network.nodes:
            self.nodes_id_name[node_.id] = node_.name

    def test_recorders(self):
        '''
           Tesing importing recorders section in pywr json
        '''
        print "Testing recorders ...................................."
        expected_recorders=[{
               "source":"NETWORK",
               "resource_attr_id":-9,
               "attr_id":2,
               "value":{
                  "name":"total_deficit",
                  "value":"{\"node\": \"Demand\", \"is_objective\": \"minimise\", \"type\": \"totaldeficitnode\"}",
                  "hidden":"N",
                  "type":"descriptor",
                  "dimension":"Dimensionless",
                  "unit":"-",
                  "metadata":"{\"pywr_section\": \"recorders\"}"
               }
            },
            {
               "source":"NETWORK",
               "resource_attr_id":-10,
               "attr_id":10,
               "value":{
                  "name":"min_volume",
                  "value":"{\"node\": \"Reservoir\", \"is_constraint\": true, \"type\": \"MinimumVolumeStorage\"}",
                  "hidden":"N",
                  "type":"descriptor",
                  "dimension":"Dimensionless",
                  "unit":"-",
                  "metadata":"{\"pywr_section\": \"recorders\"}"
               }
            }]
        imported_recorders=get_pywr_section(self.hydra_model, self.attributes_ids, self.resourcescenarios_ids,'recorders')
        compare_hydra_section(expected_recorders, imported_recorders)
       #test number of recordesr in bth of Pywr and Hydra

    def test_general_paremeters(self):
        '''
        Tesing importing parameters section in pywr json
        '''
        print "Testing paremeters ...................................."
        expected_parameters=[
            {
                "source": "NETWORK",
                "resource_attr_id": -2,
                "attr_id": 15,
                "value": {
                    "name": "demand_profile",
                    "value": "{\"comment\": \"Monthly demand profile as a factor around the mean demand\", \"values\": [0.9, 0.9, 0.9, 0.9, 1.2, 1.2, 1.2, 1.2, 0.9, 0.9, 0.9, 0.9], \"type\": \"monthlyprofile\", \"is_variable\": true}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -3,
                "attr_id": 7,
                "value": {
                    "name": "demand_saving_factor",
                    "value": "{\"comment\": \"Demand saving as a factor of the base demand\", \"params\": [{\"type\": \"constant\", \"value\": 1.0}, {\"values\": [0.95, 0.95, 0.95, 0.95, 0.9, 0.9, 0.9, 0.9, 0.95, 0.95, 0.95, 0.95], \"type\": \"monthlyprofile\"}, {\"values\": [0.5, 0.5, 0.5, 0.5, 0.4, 0.4, 0.4, 0.4, 0.5, 0.5, 0.5, 0.5], \"type\": \"monthlyprofile\"}], \"index_parameter\": \"demand_saving_level\", \"type\": \"indexedarray\"}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -4,
                "attr_id": 19,
                "value": {
                    "name": "demand_saving_level",
                    "value": "{\"comment\": \"The demand saving level\", \"control_curves\": [\"level1\", \"level2\"], \"type\": \"controlcurveindex\", \"storage_node\": \"Reservoir\"}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -5,
                "attr_id": 4,
                "value": {
                    "name": "level1",
                    "value": "{\"is_variable\": true, \"type\": \"constant\", \"value\": 0.8}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -6,
                "attr_id": 23,
                "value": {
                    "name": "level2",
                    "value": "{\"is_variable\": true, \"type\": \"constant\", \"value\": 0.5}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -7,
                "attr_id": 42,
                "value": {
                    "name": "demand_max_flow",
                    "value": "{\"agg_func\": \"product\", \"type\": \"aggregated\", \"parameters\": [\"demand_baseline\", \"demand_profile\", \"demand_saving_factor\"]}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            },
            {
                "source": "NETWORK",
                "resource_attr_id": -8,
                "attr_id": 8,
                "value": {
                    "name": "demand_baseline",
                    "value": "{\"type\": \"constant\", \"value\": 50}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"parameters\"}"
                }
            }
            ]
        imported_parameters = get_pywr_section(self.hydra_model, self.attributes_ids, self.resourcescenarios_ids,
                                           'parameters')
        compare_hydra_section(expected_parameters,imported_parameters)

    def test_nodes(self):
        '''
        test imported nodes
        '''
        print "Test nodes ......"
        no_nodes=4
        expecte_nodes_attributes=[
            {
                "source": "NODE",
                "resource_attr_id": -28,
                "attr_id": 22,
                "value": {
                    "name": "flow",
                    "value": "0.0",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            },
            {
                "source": "NODE",
                "resource_attr_id": -30,
                "attr_id": 9,
                "value": {
                    "name": "initial_volume",
                    "value": "1000",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            },
            {
                "source": "NODE",
                "resource_attr_id": -31,
                "attr_id": 21,
                "value": {
                    "name": "max_volume",
                    "value": "1000",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            },
            {
                "source": "NODE",
                "resource_attr_id": -33,
                "attr_id": 6,
                "value": {
                    "name": "cost",
                    "value": "10",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            },
            {
                "source": "NODE",
                "resource_attr_id": -35,
                "attr_id": 6,
                "value": {
                    "name": "cost",
                    "value": "-500",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            },
            {
                "source": "NODE",
                "resource_attr_id": -36,
                "attr_id": 18,
                "value": {
                    "name": "max_flow",
                    "value": "demand_max_flow",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"nodes\"}"
                }
            }
        ]
        nodes_names=["Inflow", "Reservoir", "Spill", "Demand"]
        imported_nodes = get_pywr_section(self.hydra_model, self.attributes_ids, self.resourcescenarios_ids,
                                                   'nodes')
        # test no of imported nodes
        assert len(imported_nodes)==no_nodes
        # test imported nodes names
        for node in imported_nodes:
            assert node.name in nodes_names
        imported_nodes_attributes=[]
        #for nodes_attributes in imported_nodes.values():
            #imported_nodes_attributes=imported_nodes_attributes+nodes_attributes
        compare_nodes_attributes(expecte_nodes_attributes, imported_nodes)

    def test_links(self):
        '''
        test imported links
        '''
        print "Test links ......"

        links=[["Inflow", "Reservoir"],
        ["Reservoir", "Demand"],
        ["Reservoir", "Spill"]]
        imported_links = get_pywr_section(self.hydra_model, self.attributes_ids, self.resourcescenarios_ids,
                                      'links')
        #test no of imported links
        assert len(imported_links) == len(links)
        #test link from and to nodes
        for link in imported_links:
            from_node=self.nodes_id_name[link.node_1_id]
            to_node=self.nodes_id_name[link.node_2_id]
            assert [from_node, to_node] in links



    def test_metadata(self):
        '''
                Tesing importing metadata section in pywr json
               :return:
        '''
        print "Testing metatdata ...................................."
        expected_metadata=[
            {
                "source": "NETWORK",
                "resource_attr_id": -12,
                "attr_id": 43,
                "value": {
                    "name": "metadata",
                    "value": "{\"description\": \"Demand saving using an IndexedArrayParameter with variables and constraints\", \"minimum_version\": \"0.1\", \"title\": \"Demand Saving\"}",
                    "hidden": "N",
                    "type": "descriptor",
                    "dimension": "Dimensionless",
                    "unit": "-",
                    "metadata": "{\"pywr_section\": \"metadata\"}"
                }
            }
        ]
        imported_metadata = get_pywr_section(self.hydra_model, self.attributes_ids, self.resourcescenarios_ids,
                                                   'metadata')
        compare_hydra_section(expected_metadata, imported_metadata)

    def test_domain(self):
        pass
def main():
    unittest.main()

if __name__ == '__main__':
    main()


