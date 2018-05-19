from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import generate_pywr_attributes, generate_pywr_template, pywr_template_name
import json


def test_add_network(pywr_json_filename, session_with_pywr_template, projectmaker, logged_in_client):
    client = logged_in_client

    project = projectmaker.create()

    importer = PywrHydraImporter.from_client(client, pywr_json_filename)
    importer.import_data(client, project.id)


def test_add_template(session, logged_in_client):
    client = logged_in_client  # Convenience renaming

    attributes = [a for a in generate_pywr_attributes()]

    # The response attributes have ids now.
    response_attributes = client.add_attributes(attributes)

    # Convert to a simple dict for local processing.
    attribute_ids = {a.name: a.id for a in response_attributes}

    template = generate_pywr_template(attribute_ids)
    client.add_template(template)


