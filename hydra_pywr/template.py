""" Module to generate a Hydra template from Pywr.
"""
# TODO import the other domains
from pywr.domains.river import *
from pywr.nodes import NodeMeta
import pywr


def generate_pywr_attributes():

    attribute_names = set()
    for node_name, node_klass in NodeMeta.node_registry.items():
        schema = node_klass.Schema()

        for name, field in schema.fields.items():
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

    # TODO add layout
    template = {
        'name': 'Pywr template (version: {}, git hash: {})'.format(pywr.__version__, pywr.__git_hash__),
        'templatetypes': [t for t in generate_pywr_node_templates(attribute_ids)],
    }
    return template
