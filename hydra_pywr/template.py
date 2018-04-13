""" Module to generate a Hydra template from Pywr.
"""
# TODO import the other domains
from pywr.domains.river import *
from pywr.nodes import NodeMeta
import pywr

PYWR_PROTECTED_NODE_KEYS = ('name', 'comment', 'type')


def pywr_template_name():
    """ The name of the Hydra template for Pywr. """
    return 'Pywr template (version: {}, git hash: {})'.format(pywr.__version__, pywr.__git_hash__[:6])


def generate_pywr_attributes():

    attribute_names = set()
    for node_name, node_klass in NodeMeta.node_registry.items():
        schema = node_klass.Schema()

        for name, field in schema.fields.items():
            if name in PYWR_PROTECTED_NODE_KEYS:
                continue

            if name not in attribute_names:
                yield {
                    'name': name,
                    'dimension': 'dimensionless',
                    'description': ''
                }
                attribute_names.add(name)


def generate_pywr_node_templates(attribute_ids):

    for node_name, node_klass in NodeMeta.node_registry.items():
        schema = node_klass.Schema()

        # Create an attribute for each field in the schema.
        type_attributes = []
        for name, field in schema.fields.items():
            if name in PYWR_PROTECTED_NODE_KEYS:
                continue

            type_attributes.append({
                'attr_id': attribute_ids[name],
                'description': '',
            })

        yield {
            'name': node_name,
            'resource_type': 'NODE',
            'typeattrs': type_attributes
        }


def generate_pywr_template(attribute_ids):

    template_types = [
        {
            'name': 'edge',
            'resource_type': 'LINK',
            'typeattrs': []
        },
        {
            'name': 'pywr',
            'resource_type': 'NETWORK',
            'typeattrs': []  # TODO add default network attributes
        }
    ]

    for t in generate_pywr_node_templates(attribute_ids):
        template_types.append(t)

    # TODO add layout
    template = {
        'name': pywr_template_name(),
        'templatetypes': template_types,
    }

    return template
