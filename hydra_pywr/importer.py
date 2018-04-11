import json
import warnings
from past.builtins import basestring


class PywrHydraImporter:
    PYWR_PROTECTED_NODE_KEYS = ('name', 'comment')

    def __init__(self, data):

        if isinstance(data, basestring):
            # argument is a filename
            path = data
            with open(path, "r") as f:
                data = json.load(f)
        elif hasattr(data, 'read'):
            # argument is a file-like object
            data = json.load(data)

        self.data = data

        # Default internal variables
        self.next_resource_attribute_id = -1
        self.next_node_id = -1

    @property
    def name(self):
        try:
            name = self.data['metadata']['name']
        except KeyError:
            name = 'A Pywr model.'
            warnings.warn('Pywr model data contains no name metadata. Using default name: "{}"'.format(name))
        return name

    @property
    def description(self):
        try:
            description = self.data['metadata']['description']
        except KeyError:
            description = ''
        return description

    @classmethod
    def add_attribute_group_request_data(cls, project_id):
        """ Generate the data for adding Pywr specific attribute groups to Hydra. """

        for group_name in ('metadata', 'timestepper', 'recorders', 'parameters'):
            yield {
                'project_id': project_id,
                'name': group_name,
                'description': "Pywr {} data.".format(group_name),
                'exclusive': 'Y',
            }

    def add_attributes_request_data(self):
        """ Generate the data for adding attributes to Hydra. """

        # Yield attributes from the timestepper ...
        for attr in self.attributes_from_meta():
            yield attr

        # Yield the attributes from the nodes ...
        for attr in self.attributes_from_nodes():
            yield attr

        # ... now the attributes associated with the recorders and parameters.
        for key in ('recorders', 'parameters'):
            if key not in self.data:
                continue
            for attr in self.attributes_from_component_dict(key):
                yield attr

    def add_attribute_group_items_request_data(self, attribute_ids, attribute_group_ids, network_id):
        """ Generate the data for adding attribute group items to Hydra. """

        for group_name, group_id in attribute_group_ids.items():
            for attribute_name, attribute_id in attribute_ids.items():
                if group_name not in self.data:
                    continue

                if attribute_name in self.data[group_name]:
                    # If the attribute is in one of the groups / sections of the Pywr data
                    # then we make a group item for it.
                    yield {
                        'attr_id': attribute_id,
                        'group_id': group_id,
                        'network_id': network_id,
                    }

    def add_network_request_data(self, attribute_ids, project_id, projection=None):
        """ Return a dictionary of the data required for adding a network to Hydra. """

        # TODO add additional metadata

        nodes, links, resource_scenarios = self.convert_nodes_and_edges(attribute_ids)

        network_attributes = []
        for component_key in ('recorders', 'parameters'):
            generator = self.generate_component_resource_scenarios(component_key, attribute_ids, encode_to_json=True)
            for resource_attribute, resource_scenario in generator:
                network_attributes.append(resource_attribute)
                resource_scenarios.append(resource_scenario)

        for component_key in ('metadata', 'timestepper'):
            generator = self.generate_component_resource_scenarios(component_key, attribute_ids, encode_to_json=False)
            for resource_attribute, resource_scenario in generator:
                network_attributes.append(resource_attribute)
                resource_scenarios.append(resource_scenario)

        scenario = self.make_scenario(resource_scenarios)

        data = {
            "name": self.name,
            "description": self.description,
            "project_id": project_id,
            "links": links,
            "nodes": nodes,
            "layout": None,
            "scenarios": [scenario, ],
            "projection": projection,
            "attributes": network_attributes,
            "types": [],
        }
        return data

    def make_scenario(self, resource_scenarios=None):
        """ Make the request data for a Hydra scenario. """

        if resource_scenarios is None:
            resource_scenarios = []

        timestepper = self.data['timestepper']

        scenario = {
            "name": "",
            "description": "",
            "start_time": timestepper['start'],
            "end_time": timestepper['end'],
            "time_step": str(timestepper['timestep']),
            "resourcescenarios": resource_scenarios
        }
        return scenario

    def attributes_from_nodes(self, dimension='dimensionless'):
        """ Generator to convert Pywr nodes data in to Hydra attribute data.

        This function is intended to be used to convert Pywr components (e.g. recorders, parameters, etc.)  data
        in to a format that can be imported in to Hydra. The Pywr component data is a dict of dict with each
        sub-dict represent a single component (see the "recorder" or "parameters" section of the Pywr JSON format). This
        function returns Hydra data to add a Attribute for each of the components in the outer dict.
        """
        nodes = self.data['nodes']

        attributes = set()

        for node in nodes:
            for key in node.keys():
                if key not in self.PYWR_PROTECTED_NODE_KEYS:
                    attributes.add(key)

        for attr in sorted(attributes):
            yield {
                'name': attr,
                'dimension': dimension,
                'description': ''
            }

    def attributes_from_meta(self, dimension='dimensionless'):
        """ Generator to convert Pywr timestepper data in to Hydra attribute data. """

        for meta_key in ('metadata', 'timestepper'):
            for key in self.data[meta_key].keys():
                # Prefix these names with Pywr JSON section.
                yield {
                    'name': key,
                    'dimension': dimension,
                    'description': ''
                }

    def convert_nodes_and_edges(self, attribute_ids):
        """ Convert a tuple of (nodes, links) of Hydra data based on the given Pywr data. """

        pywr_nodes = self.data['nodes']
        pywr_edges = self.data['edges']

        def find_node_id(node_name):
            for hydra_node in hydra_nodes:
                if hydra_node['name'] == node_name:
                    return hydra_node['id']
            raise ValueError('Node name "{}" not found in node data.'.format(node_name))

        node_id = -1
        link_id = -1
        hydra_nodes = []
        hydra_links = []  # Note the change in nomenclature pywr->edges, hydra->links
        hydra_resource_scenarios = []

        # First generate the hydra node data
        for pywr_node in pywr_nodes:

            try:
                comment = pywr_node['comment']
            except KeyError:
                comment = None

            # Now make the attributes
            resource_attributes = []
            for resource_attribute, resource_scenario in self.generate_node_resource_scenarios(pywr_node, attribute_ids):
                resource_attributes.append(resource_attribute)
                hydra_resource_scenarios.append(resource_scenario)

            hydra_node = {
                'id': node_id,
                'name': pywr_node['name'],
                'description': comment,
                'layout': None,
                'x': None,
                'y': None,
                'attributes': resource_attributes,
            }

            hydra_nodes.append(hydra_node)
            node_id -= 1

        for pywr_edge in pywr_edges:

            # TODO slots
            if len(pywr_edge) > 2:
                raise NotImplementedError('Edges with slot definitions are not currently supported.')

            node_1_name, node_2_name = pywr_edge

            hydra_link = {
                'id': link_id,
                'name': "{} to {}".format(node_1_name, node_2_name),
                'description': None,
                'layout': None,
                'node_1_id': find_node_id(node_1_name),
                'node_2_id': find_node_id(node_2_name),
                'attributes': [],  # Links have no resource attributes
            }
            hydra_links.append(hydra_link)
            link_id -= 1

        return hydra_nodes, hydra_links, hydra_resource_scenarios

    def _make_dataset_resource_attribute_and_scenario(self, name, value, attribute_id, dimension='dimensionless',
                                                      encode_to_json=False,):
        """ A helper method to make a dataset, resource attribute and resource scenario. """

        resource_attribute_id = self.next_resource_attribute_id
        self.next_resource_attribute_id -= 1

        # Create a dataset representing the value
        dataset = {
            'name': name,
            'value': json.dumps(value) if encode_to_json else value,
            "hidden": "N",
            "type": "descriptor",
            "dimension": dimension,
            "unit": "-",
            "metadata": "{}"
        }

        # Create a resource scenario linking the dataset to the scenario
        resource_scenario = {
            'resource_attr_id': resource_attribute_id,
            'attr_id': attribute_id,
            'value': dataset
        }

        # Create a resource attribute linking the resource scenario to the node
        resource_attribute = {
            'id': resource_attribute_id,
            'attr_id': attribute_id,
            'attr_is_var': 'N'
        }

        # Finally add the resource attribute to the hydra node data
        return resource_attribute, resource_scenario

    def generate_node_resource_scenarios(self, pywr_node, attribute_ids, dimension='dimensionless'):
        """ Generate resource attribute, resource scenario and datasets for a Pywr node.

        """
        for key in pywr_node.keys():
            if key in self.PYWR_PROTECTED_NODE_KEYS:
                continue
            # Non-protected keys represent data that must be added to Hydra.

            # Key is the attribute name. The attributes need to already by added to the
            # database and hence have a valid id.
            attribute_id = attribute_ids[key]

            yield self._make_dataset_resource_attribute_and_scenario(key, pywr_node[key], attribute_id,
                                                                     encode_to_json=True, dimension=dimension)

    def attributes_from_component_dict(self, component_key, dimension='dimensionless'):
        """ Generator to convert Pywr components data in to Hydra attribute data.

        This function is intended to be used to convert Pywr components (e.g. recorders, parameters, etc.)  data
        in to a format that can be imported in to Hydra. The Pywr component data is a dict of dict with each
        sub-dict represent a single component (see the "recorder" or "parameters" section of the Pywr JSON format). This
        function returns Hydra data to add a Attribute for each of the components in the outer dict.


        """
        components = self.data[component_key]
        for component_name in components.keys():
            yield {
                'name': component_name,
                'dimension': dimension,
                'description': ''
            }

    def generate_component_resource_scenarios(self, component_key, attribute_ids, **kwargs):
        """ Convert from Pywr components to resource attributes and resource scenarios.

        This function is intended to be used to convert Pywr components (e.g. recorders, parameters, etc.)  data
        in to a format that can be imported in to Hydra. The Pywr component data is a dict of dict with each
        sub-dict represent a single component (see the "recorder" or "parameters" section of the Pywr JSON format). This
        function returns a list of resource attributes and resource scenarios. These can be used to import the data
        to Hydra.

        """
        try:
            components = self.data[component_key]
        except KeyError:
            components = {}

        for component_name, component_data in components.items():
            # This the attribute corresponding to the component.
            # It should have a positive id and already be entered in the hydra database.
            attribute_id = attribute_ids[component_name]

            yield self._make_dataset_resource_attribute_and_scenario(component_name, component_data, attribute_id, **kwargs)

