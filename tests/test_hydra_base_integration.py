"""
The unit tests in here iteract directly with hydra-base (rather than using the Web API).
"""
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import generate_pywr_attributes, generate_pywr_template
import hydra_base
from hydra_base.lib.objects import JSONObject, Dataset


def convert_network_to_json_object(network):

    json_network = JSONObject(network)
    for scenario in json_network['scenarios']:
        for rs in scenario['resourcescenarios']:
            rs.value = Dataset(rs['value'])

    return json_network


def test_add_simple_network(simple1, session, projectmaker, root_user_id):
    project = projectmaker.create()

    importer = PywrHydraImporter(simple1)

    # First create the Pywr specific attribute groups.
    attribute_group_ids = {}
    for group_data in importer.add_attribute_group_request_data(project.project_id):
        response_group = hydra_base.add_attribute_group(JSONObject(group_data), user_id=root_user_id)
        attribute_group_ids[group_data['name']] = response_group.id

    # First the attributes must be added.
    attributes = [JSONObject(a) for a in importer.add_attributes_request_data()]

    # The response attributes have ids now.
    response_attributes = hydra_base.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.attr_name: a.attr_id for a in response_attributes}

    # Now we try to create the network
    network = importer.add_network_request_data(attribute_ids, project.project_id)
    json_network = convert_network_to_json_object(network)

    hydra_base.add_network(json_network, user_id=root_user_id)


def test_add_template(session, root_user_id):

    attributes = [JSONObject(a) for a in generate_pywr_attributes()]

    # The response attributes have ids now.
    response_attributes = hydra_base.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.attr_name: a.attr_id for a in response_attributes}

    template = generate_pywr_template(attribute_ids)

    hydra_base.add_template(JSONObject(template))
