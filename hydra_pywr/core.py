import json


class BasePywrHydra:
    def __init__(self):
        # Default internal variables
        self.next_resource_attribute_id = -1

    def _make_dataset_resource_scenario(self, name, value, resource_attribute_id, data_type='descriptor',
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

    def _make_dataset_resource_attribute_and_scenario(self, name, value, attribute_id, dimension='dimensionless',
                                                      encode_to_json=False,):
        """ A helper method to make a dataset, resource attribute and resource scenario. """

        resource_attribute_id = self.next_resource_attribute_id
        self.next_resource_attribute_id -= 1

        resource_scenario = self._make_dataset_resource_scenario(name, value, resource_attribute_id, dimension=dimension,
                                                                 encode_to_json=encode_to_json)

        # Create a resource attribute linking the resource scenario to the node
        resource_attribute = {
            'id': resource_attribute_id,
            'attr_id': attribute_id,
            'attr_is_var': 'N'
        }

        # Finally return resource attribute and resource scenario
        return resource_attribute, resource_scenario

