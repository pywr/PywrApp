"""
The unit tests in here interact directly with hydra-base (rather than using the Web API).
"""
from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.exporter import PywrHydraExporter
from hydra_pywr.template import pywr_template_name
import hydra_base
import pytest
import json


def assert_identical_pywr_node_data(node1, node2):
    assert sorted(node1.keys()) == sorted(node2.keys())
    for node_key in node1.keys():
        if node_key == 'type':
            # Types can be upper or lower case
            assert node1[node_key].lower() == node2[node_key].lower()
        else:
            assert node1[node_key] == node2[node_key]


def assert_identical_pywr_data(data1, data2):
    """ Assert two Pywr JSON data dictionaries are identical. """
    assert sorted(data1.keys()) == sorted(data2.keys())

    for key in data1.keys():
        if key == 'nodes':
            # The ordering of these lists does not matter to Pywr
            for node1, node2 in zip(sorted(data1[key], key=lambda n: n['name']), sorted(data2[key], key=lambda n: n['name'])):
                assert_identical_pywr_node_data(node1, node2)
        elif key == 'edges':
            # The ordering of these lists does not matter to Pywr
            assert sorted(data1[key]) == sorted(data2[key])
        else:
            assert data1[key] == data2[key]


@pytest.fixture()
def db_with_pywr_network(pywr_json_filename, session_with_pywr_template, projectmaker, root_user_id):
    project = projectmaker.create()

    template = JSONObject(hydra_base.get_template_by_name(pywr_template_name()))

    importer = PywrHydraImporter(pywr_json_filename, template)

    # First create the Pywr specific attribute groups.
    attribute_group_ids = {}
    for group_data in importer.add_attribute_group_request_data(project.id):
        response_group = hydra_base.add_attribute_group(JSONObject(group_data), user_id=root_user_id)
        attribute_group_ids[group_data['name']] = response_group.id

    # First the attributes must be added.
    attributes = [JSONObject(a) for a in importer.add_attributes_request_data()]

    # The response attributes have ids now.
    response_attributes = hydra_base.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.name: a.id for a in response_attributes}

    # Now we try to create the network
    network = importer.add_network_request_data(attribute_ids, project.id)
    json_network = convert_network_to_json_object(network)

    hydra_network = hydra_base.add_network(json_network, user_id=root_user_id)

    # Now we have to add the attribute group items
    attribute_group_items = importer.add_attribute_group_items_request_data(attribute_ids, attribute_group_ids,
                                                                            hydra_network.id)

    hydra_base.add_attribute_group_items([JSONObject(i) for i in attribute_group_items], user_id=root_user_id)

    return hydra_network.id, pywr_json_filename


def test_export(db_with_pywr_network, root_user_id):
    pywr_network_id, pywr_json_filename = db_with_pywr_network

    # Fetch the network
    network = hydra_base.get_network(pywr_network_id, user_id=root_user_id, include_data='Y')
    # Fetch all the attributes
    attributes = hydra_base.get_attributes()
    # TODO not sure why this returns SQLAlchemy object?
    # TODO rename this to map/lookup
    attributes = {attr.id: JSONObject(attr) for attr in attributes}

    # Fetch all the attribute group items for this network
    # TODO rename this to map/lookup
    attribute_group_items_i = hydra_base.get_network_attributegroup_items(pywr_network_id, user_id=root_user_id)
    attribute_group_items_j = [JSONObject(a) for a in attribute_group_items_i]

    # TODO this can be removed when JSONObject is fixed to load the group data from within the group item.
    for attribute_group_item_i, attribute_group_item_j in zip(attribute_group_items_i, attribute_group_items_j):
        group_j = JSONObject(attribute_group_item_i.group)
        attribute_group_item_j.group = group_j

    # We also need the template to get the node types
    template = JSONObject(hydra_base.get_template_by_name(pywr_template_name()))

    exporter = PywrHydraExporter(network, attributes, attribute_group_items_j, template)

    pywr_data_exported = exporter.get_pywr_data()

    # Check transformed data is about right
    with open(pywr_json_filename) as fh:
        pywr_data = json.load(fh)

    assert_identical_pywr_data(pywr_data, pywr_data_exported)


