import click
import os
from xml.etree import ElementTree as ET
from xml.dom.minidom import parseString
import json
from hydra_client.connection import JSONConnection
from .exporter import PywrHydraExporter
from .runner import PywrHydraRunner
from .importer import PywrHydraImporter
from .util import make_plugins
from .template import register_template


# TODO replace with a generic client loader from hydra_client
# TODO get hydra_client to handle the authentication stuff
def get_client():
    return JSONConnection(app_name='Pywr Hydra App')


def start_cli():
    cli(obj={})


@click.group()
@click.pass_obj
@click.option('-u', '--user', type=str, default=None)
@click.option('-p', '--password', type=str, default=None)
def cli(obj, user, password):
    """ CLI for the Pywr-Hydra application. """
    client = get_client()
    client.login(username=user, password=password)
    obj['client'] = client


@cli.command(name='import')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.argument('project_id', type=int)
def import_json(obj, filename, project_id):
    """ Import a Pywr JSON file into Hydra. """
    client = obj['client']
    importer = PywrHydraImporter.from_client(client, filename)
    importer.import_data(client, project_id)


@cli.command(name='export')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('--json-indent', type=int, default=2)
@click.option('--json-sort-keys', type=int, default=True)
def export_json(obj, filename, network_id, sort_keys, indent):
    """ Export a Pywr JSON from Hydra. """
    client = obj['client']
    exporter = PywrHydraExporter.from_network_id(client, network_id)

    with open(filename, mode='w') as fh:
        json.dump(exporter.get_pywr_data(), fh, sort_keys=sort_keys, indent=indent)


@cli.command()
@click.pass_obj
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
def run(obj, network_id, scenario_id):
    """ Export, run and save a Pywr model from Hydra. """

    client = obj['client']
    runner = PywrHydraRunner.from_network_id(client, network_id, scenario_id)

    runner.load_pywr_model()

    runner.run_pywr_model()

    runner.save_pywr_results()


@cli.command()
@click.pass_obj
def register(obj):
    """ Register the app with the Hydra installation. """
    import hydra_base

    plugins = make_plugins(cli)

    base_plugin_dir = hydra_base.config.get('plugin', 'default_directory')

    base_plugin_dir = os.path.join(base_plugin_dir, 'PywrApp')

    if not os.path.exists(base_plugin_dir):
        os.mkdir(base_plugin_dir)

    for name, element in plugins:
        plugin_path = os.path.join(base_plugin_dir, name)

        if name == 'register':
            continue  # Don't register this command

        if not os.path.exists(plugin_path):
            os.mkdir(plugin_path)

        with open(os.path.join(plugin_path, 'plugin.xml'), 'w') as fh:
            reparsed = parseString(ET.tostring(element, 'utf-8'))
            fh.write(reparsed.toprettyxml(indent="\t"))

        # We also need to write a very basic script to run the command
        with open(os.path.join(plugin_path, 'run.sh'), 'w') as fh:
            fh.writelines([
                '#!/bin/bash\n',
                'hydra-pywr "$@"\n',
            ])


@cli.group()
def template():
    pass


@template.command('register')
@click.pass_obj
def template_register(obj):
    """ Register a Pywr template with Hydra. """

    client = obj['client']
    register_template(client)
