"""
The unit tests in this module test the internal behaviour of the Pywr-Hydra application.

"""
import pytest
import json
from hydra_pywr import attributes_from_component_dict, data_from_component_dict


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
def hydra_attribute_data():
    """ Example attribute data

    This data looks like the returned value from adding or getting attributes from Hydra.
    """
    return [
        {
            "id": 1,
            "name": "my_component",
            "dimension": "recorder",
            "description": ""
        },
        {
            "id": 2,
            "name": "my_other_component",
            "dimension": "recorder",
            "description": ""
        },
    ]


@pytest.fixture()
def hydra_attribute_ids(hydra_attribute_data):
    return {d["name"]: d["id"] for d in hydra_attribute_data}


def test_components_to_attributes(pywr_component_data):
    """ Test converting a dict of component data to the format for a adding Hydra attributes """

    hydra_data = attributes_from_component_dict(pywr_component_data, dimension='recorder')

    # There should be a hydra attribute for each entry in the pywr data
    assert len(list(hydra_data)) == len(pywr_component_data)

    pywr_keys = list(pywr_component_data.keys())

    for data in hydra_data:
        assert data['name'] in pywr_keys
        assert data['dimensions'] == 'recorder'
        assert data['description'] == ''

        
def test_components_to_datasets(pywr_component_data, hydra_attribute_ids):
    """ Test converting a dict of component data to the format for adding Hydra resource data """

    resource_attributes, resource_scenarios = data_from_component_dict(pywr_component_data, hydra_attribute_ids)

    # There should be one resource attribute and scenario dataset for each component
    assert len(resource_attributes) == len(resource_scenarios) == len(pywr_component_data) == len(hydra_attribute_ids)

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

        # This the raw data that should be encoded to a JSON string and become the
        # "value" in the Hydra dataset.
        expected_data = pywr_component_data[attr_name]

        # This should be that JSON string.
        value_json = data['value']

        # If we load back from JSON we can compare
        # Note we can't compare the JSON strings due to formatting and ordering etc.
        value_data = json.loads(value_json)

        assert value_data == expected_data









