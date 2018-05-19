from helpers import *
from fixtures import *
from hydra_base_fixtures import *
from hydra_pywr.exporter import PywrHydraExporter
from hydra_pywr.template import pywr_template_name
import json


def test_export(db_with_pywr_network, logged_in_client):
    client = logged_in_client

    pywr_network_id, pywr_json_filename = db_with_pywr_network
    exporter = PywrHydraExporter.from_network_id(client, pywr_network_id)
    pywr_data_exported = exporter.get_pywr_data()

    # Check transformed data is about right
    with open(pywr_json_filename) as fh:
        pywr_data = json.load(fh)

    assert_identical_pywr_data(pywr_data, pywr_data_exported)


