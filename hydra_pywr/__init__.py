import json


def attributes_from_component_dict(components, dimension=''):
    """ Generator to convert Pywr components data in to Hydra attribute data.

    This function is intended to be used to convert Pywr components (e.g. recorders, parameters, etc.)  data
    in to a format that can be imported in to Hydra. The Pywr component data is a dict of dict with each
    sub-dict represent a single component (see the "recorder" or "parameters" section of the Pywr JSON format). This
    function returns Hydra data to add a Attribute for each of the components in the outer dict.


    """
    for component_name in components.keys():
        yield {
            'name': component_name,
            'dimension': dimension,
            'description': ''
        }


def data_from_component_dict(components, attribute_ids):
    """ Convert from Pywr components to resource attributes and resource scenarios.

    This function is intended to be used to convert Pywr components (e.g. recorders, parameters, etc.)  data
    in to a format that can be imported in to Hydra. The Pywr component data is a dict of dict with each
    sub-dict represent a single component (see the "recorder" or "parameters" section of the Pywr JSON format). This
    function returns a list of resource attributes and resource scenarios. These can be used to import the data
    to Hydra.

    """
    resource_attributes = []
    resource_scenarios = []

    # TODO this needs to be configurable
    resource_attribute_id = -1
    for component_name, component_data in components.items():

        # This the attribute corresponding to the component.
        # It should have a positive id and already be entered in the hydra database.
        attribute_id = attribute_ids[component_name]

        # Make the resource attribute
        resource_attribute = {
            'id': resource_attribute_id,
            'attr_id': attribute_id,
            'attr_is_var': 'N'
        }

        # Make the resource scenario

        # TODO the example has a "source" key that's not used here.
        # TODO possible other metadata to add.
        resource_scenario = {
            'resource_attr_id': resource_attribute_id,
            'attr_id': attribute_id,
            'value': json.dumps(component_data)
        }

        resource_attributes.append(resource_attribute)
        resource_scenarios.append(resource_scenario)

        resource_attribute_id -= 1

    return resource_attributes, resource_scenarios
