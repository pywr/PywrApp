from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.importer import PywrHydraImporter
from hydra_pywr.template import generate_pywr_attributes, generate_pywr_template, pywr_template_name, register_template
import json


def test_add_network(pywr_json_filename, session_with_pywr_template, projectmaker, logged_in_client):
    client = logged_in_client

    project = projectmaker.create()

    importer = PywrHydraImporter.from_client(client, pywr_json_filename)
    importer.import_data(client, project.id)


def test_add_template(session, logged_in_client):
    register_template(logged_in_client)


