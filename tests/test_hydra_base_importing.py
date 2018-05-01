"""
The unit tests in here interact directly with hydra-base (rather than using the Web API).
"""
from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import generate_pywr_attributes, generate_pywr_template, pywr_template_name
import hydra_base
import pytest
import json


def assert_dataset(hydra_data, key, value, decode_from_json=False):
    """ Raise an assertion error if the value is not found as a resource scenario's dataset. """

    for scenario in hydra_data['scenarios']:
        for rs in scenario['resourcescenarios']:
            hydra_value = rs['value']['value']
            if decode_from_json:
                # The data in hydra is stored as a JSON encoded string.
                # Load it back to Python types for comparison with Pywr data.
                hydra_value = json.loads(hydra_value)
            if hydra_value == value:
                return
    raise AssertionError('Dataset "{}" value not found in the hydra resource scenario data.'.format(key))


def assert_hydra_pywr(hydra_data, pywr_data):
    """ Compare Hydra and Pywr data for a model and raise an `AssertionError` if they are different.

    This is a convenience function for testing purposes. It compares Hydra data in the
    format suitable to send to an add network request with the Pywr JSON data.

    """

    assert len(hydra_data['nodes']) == len(pywr_data['nodes'])
    assert len(hydra_data['links']) == len(pywr_data['edges'])

    # Check coordinates
    for pywr_node in pywr_data['nodes']:
        for hydra_node in hydra_data['nodes']:
            if pywr_node['name'] == hydra_node['name']:
                try:
                    pywr_coordinate = pywr_node['position']['geographic']
                except KeyError:
                    pass
                else:
                    assert pywr_coordinate == [hydra_node['x'], hydra_node['y']]

    # Ensure that the time-stepper information exists.
    timestepper = pywr_data['timestepper']
    for key, value in timestepper.items():
        assert_dataset(hydra_data, key, value)

    metadata = pywr_data['metadata']
    for key, value in metadata.items():
        assert_dataset(hydra_data, key, value)

    # Ensure that the pywr parameters exist as a hydra dataset
    for component in ('parameters', 'recorders'):
        if component in pywr_data:
            for component_name, component_value in pywr_data[component].items():
                assert_dataset(hydra_data, component_name, component_value, decode_from_json=True)


def test_add_network(pywr_json_filename, session_with_pywr_template, projectmaker, root_user_id):
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
    # TODO change this variable name to map or lookup
    attribute_ids = {a.attr_name: a.attr_id for a in response_attributes}

    # Now we try to create the network
    network = importer.add_network_request_data(attribute_ids, project.id)

    # Check transformed data is about right
    with open(pywr_json_filename) as fh:
        pywr_data = json.load(fh)

    assert_hydra_pywr(network, pywr_data)

    json_network = convert_network_to_json_object(network)

    hydra_network = hydra_base.add_network(json_network, user_id=root_user_id)

    # Now we have to add the attribute group items
    attribute_group_items = importer.add_attribute_group_items_request_data(attribute_ids, attribute_group_ids,
                                                                            hydra_network.id)

    hydra_base.add_attribute_group_items([JSONObject(i) for i in attribute_group_items], user_id=root_user_id)


def test_add_template(session, root_user_id):

    attributes = [JSONObject(a) for a in generate_pywr_attributes()]

    # The response attributes have ids now.
    response_attributes = hydra_base.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.attr_name: a.attr_id for a in response_attributes}

    template = generate_pywr_template(attribute_ids)

    hydra_base.add_template(JSONObject(template))
