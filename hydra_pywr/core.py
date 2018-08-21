import json
from hydra_base.lib.HydraTypes.Types import Scalar, Array
from hydra_pywr_data_types import PywrParameter, PywrRecorder, PYWR_DATA_TYPE_MAP
from pywr.schema.fields import ParameterField, ParameterReferenceField, ParameterValuesField
from marshmallow.fields import Number, Integer, List


def data_type_from_field(field):
    """ Return the appropriate Hydra DataType for a given node's attribute. """

    if isinstance(field, (ParameterReferenceField, ParameterField)):
        data_type = PywrParameter.tag
    elif isinstance(field, ParameterValuesField):
        # TODO support this data type properly.
        data_type = Scalar.tag
    elif isinstance(field, (Number, Integer)):
        data_type = Scalar.tag
    elif isinstance(field, List):
        data_type = Array.tag
    else:
        raise ValueError('No data type found for field: {}'.format(field))

    return data_type


def data_type_from_component(component_group, component_name):
    # Determine the data type
    if component_group in ('parameters', 'recorders'):
        data_type = PYWR_DATA_TYPE_MAP[component_group].tag
    else:
        if component_group == 'timestepper' and component_name == 'timestep':
            data_type = 'SCALAR'
        else:
            data_type = 'DESCRIPTOR'
            
    return data_type


class BasePywrHydra:
    def __init__(self):
        # Default internal variables
        self.next_resource_attribute_id = -1

    def _make_dataset_resource_scenario(self, name, value, data_type, resource_attribute_id,
                                        encode_to_json=False,):
        """ A helper method to make a dataset, resource attribute and resource scenario. """

        # Create a dataset representing the value
        dataset = {
            'name': name,
            'value': json.dumps(value) if encode_to_json else value,
            "hidden": "N",
            "type": data_type,
            "unit": "-",
            "metadata": json.dumps({'json_encoded': encode_to_json})
        }

        # Create a resource scenario linking the dataset to the scenario
        resource_scenario = {
            'resource_attr_id': resource_attribute_id,
            'dataset': dataset
        }

        # Finally return resource attribute and resource scenario
        return resource_scenario

    def _make_dataset_resource_attribute_and_scenario(self, name, value, data_type, attribute_id, **kwargs):
        """ A helper method to make a dataset, resource attribute and resource scenario. """

        resource_attribute_id = self.next_resource_attribute_id
        self.next_resource_attribute_id -= 1

        resource_scenario = self._make_dataset_resource_scenario(name, value, data_type, resource_attribute_id, **kwargs)

        # Create a resource attribute linking the resource scenario to the node
        resource_attribute = {
            'id': resource_attribute_id,
            'attr_id': attribute_id,
            'attr_is_var': 'N'
        }

        # Finally return resource attribute and resource scenario
        return resource_attribute, resource_scenario

