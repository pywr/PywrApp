import os
import pytest
import hydra_base
from helpers import convert_network_to_json_object
from hydra_base import JSONObject
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import pywr_template_name
from hydra_client.connection import JSONConnection


@pytest.fixture()
def client():
    return JSONConnection(app_name="Test Pywr application.")


@pytest.fixture()
def logged_in_client(client):
    root_user_id = client.login('root', '')
    return client



@pytest.fixture()
def model_directory():
    return os.path.join(os.path.dirname(__file__), 'models')


@pytest.fixture()
def simple1(model_directory):
    return os.path.join(model_directory, 'simple1.json')


@pytest.fixture(params=[
    'simple1.json',
    'reservoir2.json',
    'parameter_reference.json',
])
def pywr_json_filename(request, model_directory):
    return os.path.join(model_directory, request.param)


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
    for n, i in sorted(attribute_ids.items()):
        print(n, i)

    # Now we try to create the network
    network = importer.add_network_request_data(attribute_ids, project.id)
    json_network = convert_network_to_json_object(network)

    hydra_network = hydra_base.add_network(json_network, user_id=root_user_id)

    # Now we have to add the attribute group items
    attribute_group_items = importer.add_attribute_group_items_request_data(attribute_ids, attribute_group_ids,
                                                                            hydra_network.id)

    hydra_base.add_attribute_group_items([JSONObject(i) for i in attribute_group_items], user_id=root_user_id)

    return hydra_network.id, pywr_json_filename
