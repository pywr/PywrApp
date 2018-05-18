from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.exporter import PywrHydraExporter
from hydra_pywr.template import pywr_template_name
import json


def test_export(db_with_pywr_network, logged_in_client):
    client = logged_in_client

    pywr_network_id, pywr_json_filename = db_with_pywr_network

    # Fetch the network
    network = client.get_network(pywr_network_id, include_data='Y')
    # Fetch all the attributes
    attributes = client.get_attributes()
    # TODO not sure why this returns SQLAlchemy object?
    # TODO rename this to map/lookup
    attributes = {attr.id: attr for attr in attributes}

    # Fetch all the attribute group items for this network
    # TODO rename this to map/lookup
    attribute_group_items = client.get_network_attributegroup_items(pywr_network_id)

    # # TODO this can be removed when JSONObject is fixed to load the group data from within the group item.
    for attribute_group_item in attribute_group_items:
        group = client.get_attribute_group(attribute_group_item['group_id'])
        assert group['id'] == attribute_group_item['group_id']
        attribute_group_item['group'] = group

    # We also need the template to get the node types
    template = client.get_template_by_name(pywr_template_name())

    exporter = PywrHydraExporter(network, attributes, attribute_group_items, template)

    pywr_data_exported = exporter.get_pywr_data()

    # Check transformed data is about right
    with open(pywr_json_filename) as fh:
        pywr_data = json.load(fh)

    assert_identical_pywr_data(pywr_data, pywr_data_exported)


