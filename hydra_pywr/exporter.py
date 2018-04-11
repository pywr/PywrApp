import json
from past.builtins import basestring


class PywrHydraExporter:
    def __init__(self, data, attributes, attribute_group_items, template):
        self.data = data
        self.attributes = attributes
        self.attribute_group_items = attribute_group_items
        self.template = template

    def get_pywr_data(self):

        pywr_data = {}

        # TODO see proposed changes to metadata and timestepper data.
        for group_name in ('metadata', 'timestepper', 'recorders', 'parameters'):
            # Recorders and parameters are JSON encoded.
            decode_from_json = group_name in ('recorders', 'parameters')

            group_data = {}
            for key, value in self.generate_group_data(group_name, decode_from_json=decode_from_json):
                group_data[key] = value

            # Only make the section if it contains data.
            if len(group_data) > 0:
                pywr_data[group_name] = group_data

        nodes = []
        for node in self.generate_pywr_nodes():
            nodes.append(node)
        pywr_data['nodes'] = nodes

        edges = []
        for edge in self.generate_pywr_edges():
            edges.append(edge)
        pywr_data['edges'] = edges

        return pywr_data

    def _get_attribute_group_from_name(self, group_name):

        for attribute_group_item in self.attribute_group_items:
            group = attribute_group_item['group']
            if group['name'] == group_name:
                return group

        raise ValueError('No attribute group found for group name: {}'.format(group_name))

    def _get_attributes_for_group_from_name(self, group_name):

        group = self._get_attribute_group_from_name(group_name)

        for attribute_group_item in self.attribute_group_items:
            if attribute_group_item['group_id'] == group['id']:
                yield self.attributes[attribute_group_item['attr_id']]

    def _get_resource_scenario(self, resource_attribute_id):

        # TODO this just returns the first resource scenario that is found.
        for scenario in self.data['scenarios']:
            for resource_scenario in scenario['resourcescenarios']:
                if resource_scenario['resource_attr_id'] == resource_attribute_id:
                    return resource_scenario

        raise ValueError('No resource scenario found for resource attribute id: {}'.format(resource_attribute_id))

    def _get_node(self, node_id):

        for node in self.data['nodes']:
            if node['id'] == node_id:
                return node

        raise ValueError('No node found with node_id: {}'.format(node_id))

    def generate_pywr_nodes(self):
        """ Generator returning a Pywr dict for each node in the network. """

        for node in self.data['nodes']:

            # Create the basic information.
            pywr_node = {'name': node['name']}

            if node['description'] is not None:
                pywr_node['comment'] = node['description']

            # Get the type for this node from the template
            pywr_node_type = None
            for node_type in node['types']:
                for template_type in self.template['templatetypes']:
                    if node_type['type_id'] == template_type['type_id']:
                        pywr_node_type = template_type['type_name']
            if pywr_node_type is None:
                raise ValueError('Template does not contain node of type "{}".'.format(pywr_node_type))

            pywr_node['type'] = pywr_node_type

            # Then add any corresponding attributes / data
            for resource_attribute in node['attributes']:
                attribute = self.attributes[resource_attribute['attr_id']]
                try:
                    resource_scenario = self._get_resource_scenario(resource_attribute['resource_attr_id'])
                except ValueError:
                    continue  # No data associated with this attribute.
                dataset = resource_scenario['dataset']
                value = dataset['value']

                if isinstance(value, basestring):
                    pywr_node[attribute['name']] = json.loads(value)
                else:
                    pywr_node[attribute['name']] = value

            yield pywr_node

    def generate_pywr_edges(self):
        """ Generator returning a Pywr tuple for each link/edge in the network. """

        for link in self.data['links']:
            node_from = self._get_node(link['node_1_id'])
            node_to = self._get_node(link['node_2_id'])
            yield [node_from['name'], node_to['name']]

    def generate_group_data(self, group_name, decode_from_json=False):
        """ Generator returning a key and dict value for meta keys. """

        # These are all the attributes associated with the group
        try:
            group_attributes = list(self._get_attributes_for_group_from_name(group_name))
        except ValueError:
            # No group exists so there is no data
            return

        for resource_attribute in self.data['attributes']:

            attribute = self.attributes[resource_attribute['attr_id']]

            attribute_name = attribute['name']

            for group_attribute in group_attributes:
                # Find if this attribute is in this group
                if group_attribute['id'] == attribute['id']:
                    break
            else:
                continue  # Filter out keys not associated the group

            resource_scenario = self._get_resource_scenario(resource_attribute['resource_attr_id'])
            dataset = resource_scenario['dataset']
            value = dataset['value']

            if decode_from_json:
                value = json.loads(value)

            # TODO check this. It should not happen as described below.
            # Hydra opportunistically converts everything to native types
            # Some of the Pywr data should remain as string despite looking like a float/int
            if attribute_name == 'minimum_version' and group_name == 'metadata':
                value = str(value)

            yield attribute_name, value





