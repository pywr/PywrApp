"""
The unit tests in this module test the internal behaviour of the Pywr-Hydra application.

"""
import pytest
import json
from hydra_pywr.importer import PywrHydraImporter


@pytest.fixture()
def pywr_component_data():
    """ Example component data.

    This data looks like the "recorders" or "parameters" section of a Pywr JSON file.
    """
    return {
        "my_component": {
            "type": "the_best_component",
            "attribute": "awesome"
        },
        "my_other_component": {
            "type": "the_worst_component",
            "attribute": "terrible"
        }
    }


@pytest.fixture()
def pywr_component_importer(pywr_component_data):
    return PywrHydraImporter({'recorders': pywr_component_data})


@pytest.fixture()
def hydra_attribute_data():
    """ Example attribute data

    This data looks like the returned value from adding or getting attributes from Hydra.
    """
    return [
        {
            "id": 1,
            "name": "my_component",
            "dimension": "dimensionless",
            "description": ""
        },
        {
            "id": 2,
            "name": "my_other_component",
            "dimension": "dimensionless",
            "description": ""
        },
    ]


@pytest.fixture()
def hydra_attribute_ids(hydra_attribute_data):
    return {d["name"]: d["id"] for d in hydra_attribute_data}


def test_components_to_attributes(pywr_component_importer):
    """ Test converting a dict of component data to the format for a adding Hydra attributes """
    importer = pywr_component_importer

    hydra_data = importer.attributes_from_component_dict('recorders')

    # There should be a hydra attribute for each entry in the pywr data
    assert len(list(hydra_data)) == len(importer.data['recorders'])

    pywr_keys = list(importer.data['recorders'].keys())

    for data in hydra_data:
        assert data['name'] in pywr_keys
        assert data['dimensions'] == 'dimensionless'
        assert data['description'] == ''

        
def test_components_to_datasets(pywr_component_importer, hydra_attribute_ids):
    """ Test converting a dict of component data to the format for adding Hydra resource data """
    importer = pywr_component_importer

    resource_attributes = []
    resource_scenarios = []
    for ra, rs in importer.generate_component_resource_scenarios('recorders', hydra_attribute_ids,
                                                   dimension='recorder'):
        resource_scenarios.append(rs)
        resource_attributes.append(ra)


    # There should be one resource attribute and scenario dataset for each component
    assert len(resource_attributes) == len(resource_scenarios) == len(importer.data['recorders']) == len(hydra_attribute_ids)

    # Check the attributes first
    for data in resource_attributes:
        # This should be "N" because none of this data is variable from Hydra's perspective.
        assert data['attr_is_var'] == 'N'

        # id should be negative because it is new.
        assert data['id'] < 0

        # attr_id should be positive and one of the ones in the hydra_attribute_ids
        assert data['attr_id'] > 0
        assert data['attr_id'] in list(hydra_attribute_ids.values())

    for data in resource_scenarios:

        # Find the attribute linked to this data; there should only be one.
        num_found = 0
        for attr_data in resource_attributes:
            if attr_data['id'] == data['resource_attr_id']:
                num_found += 1
        # And ensure we found at exactly one.
        assert num_found == 1

        # Find the attribute name for this data
        attr_name = None
        for name, attr_id in hydra_attribute_ids.items():
            if data['attr_id'] == attr_id:
                attr_name = name
                break
        else:
            raise ValueError('No attributes with id "{}" found.'.format(data['attr_id']))

        # The dataset should be encoded in the "value" key of resource scenario
        dataset_data = data['value']

        dataset_data['type'] = 'descriptor'
        dataset_data['dimension'] = 'recorder'
        dataset_data['hidden'] = 'N'
        dataset_data['unit'] = '-'
        dataset_data['metadata'] = '{}'
        dataset_data['name'] = attr_name

        # This should be that JSON string.
        value_json = dataset_data['value']

        # This the raw data that should be encoded to a JSON string and become the
        # "value" in the Hydra dataset.
        expected_data = importer.data['recorders'][attr_name]

        # If we load back from JSON we can compare
        # Note we can't compare the JSON strings due to formatting and ordering etc.
        value_data = json.loads(value_json)

        assert value_data == expected_data









