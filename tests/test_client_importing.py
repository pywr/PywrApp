from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import generate_pywr_attributes, generate_pywr_template, pywr_template_name
import json


def test_add_network(pywr_json_filename, session_with_pywr_template, projectmaker, logged_in_client):
    client = logged_in_client

    project = projectmaker.create()

    template = client.get_template_by_name(pywr_template_name())
    importer = PywrHydraImporter(pywr_json_filename, template)

    # First create the Pywr specific attribute groups.
    attribute_group_ids = {}
    for group_data in importer.add_attribute_group_request_data(project.id):
        response_group = client.add_attribute_group(group_data)
        attribute_group_ids[group_data['name']] = response_group.id

    # First the attributes must be added.
    attributes = [a for a in importer.add_attributes_request_data()]
    attributes = importer.add_attributes_request_data()

    # The response attributes have ids now.
    response_attributes = client.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    # TODO change this variable name to map or lookup
    attribute_ids = {a.name: a.id for a in response_attributes}

    # Now we try to create the network
    network = importer.add_network_request_data(attribute_ids, project.id)

    # Check transformed data is about right
    with open(pywr_json_filename) as fh:
        pywr_data = json.load(fh)

    assert_hydra_pywr(network, pywr_data)

    json_network = convert_network_to_json_object(network)

    hydra_network = client.add_network(json_network)

    # Now we have to add the attribute group items
    attribute_group_items = importer.add_attribute_group_items_request_data(attribute_ids, attribute_group_ids,
                                                                            hydra_network.id)

    client.add_attribute_group_items(attribute_group_items)


def test_add_template(session, logged_in_client):
    client = logged_in_client  # Convenience renaming

    attributes = [a for a in generate_pywr_attributes()]

    # The response attributes have ids now.
    response_attributes = client.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.name: a.id for a in response_attributes}

    template = generate_pywr_template(attribute_ids)
    client.add_template(template)


