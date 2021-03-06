import click
import os
from pathlib import Path
from shutil import copyfile
from xml.etree import ElementTree as ET
from xml.dom.minidom import parseString
import json
from hydra_client.connection import JSONConnection
from .exporter import PywrHydraExporter
from .runner import PywrHydraRunner
from .importer import PywrHydraImporter
from .util import make_plugins
from .template import register_template, unregister_template


def hydra_app(category='import'):
    def hydra_app_decorator(func):
        func.hydra_app_category = category
        return func
    return hydra_app_decorator


def get_client(hostname, **kwargs):
    return JSONConnection(app_name='Pywr Hydra App', db_url=hostname, **kwargs)


def get_logged_in_client(context, user_id=None):
    session = context['session']
    client = get_client(context['hostname'], session_id=session, user_id=user_id)
    if client.user_id is None:
        client.login(username=context['username'], password=context['password'])
    return client


def start_cli():
    cli(obj={}, auto_envvar_prefix='HYDRA_PYWR')


@click.group()
@click.pass_obj
@click.option('-u', '--username', type=str, default=None)
@click.option('-p', '--password', type=str, default=None)
@click.option('-h', '--hostname', type=str, default=None)
@click.option('-s', '--session', type=str, default=None)
def cli(obj, username, password, hostname, session):
    """ CLI for the Pywr-Hydra application. """

    obj['hostname'] = hostname
    obj['username'] = username
    obj['password'] = password
    obj['session'] = session


@hydra_app()
@cli.command(name='import')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.argument('project_id', type=int)
@click.option('-u', '--user-id', type=int, default=None)
def import_json(obj, filename, project_id, user_id):
    """ Import a Pywr JSON file into Hydra. """
    client = get_logged_in_client(obj, user_id=user_id)
    importer = PywrHydraImporter.from_client(client, filename)
    importer.import_data(client, project_id)


@hydra_app(category='export')
@cli.command(name='export')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('--json-indent', type=int, default=2)
@click.option('--json-sort-keys', type=int, default=True)
def export_json(obj, filename, network_id, scenario_id, user_id, json_sort_keys, json_indent):
    """ Export a Pywr JSON from Hydra. """
    client = get_logged_in_client(obj, user_id=user_id)
    exporter = PywrHydraExporter.from_network_id(client, network_id, scenario_id)

    with open(filename, mode='w') as fh:
        json.dump(exporter.get_pywr_data(), fh, sort_keys=json_sort_keys, indent=json_indent)


@hydra_app(category='model')
@cli.command()
@click.pass_obj
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def run(obj, network_id, scenario_id, user_id):
    """ Export, run and save a Pywr model from Hydra. """

    client = get_logged_in_client(obj, user_id=user_id)
    runner = PywrHydraRunner.from_network_id(client, network_id, scenario_id)

    runner.load_pywr_model()

    runner.run_pywr_model()

    runner.save_pywr_results(client)


@cli.command()
@click.pass_obj
@click.argument('docker-image', type=str)
@click.argument('docker-tag', type=str)
def register(obj, docker_image, docker_tag):
    """ Register the app with the Hydra installation. """
    import hydra_base

    plugins = make_plugins(cli, '{}:{}'.format(docker_image, docker_tag))

    base_plugin_dir = Path(hydra_base.config.get('plugin', 'default_directory'))

    base_plugin_dir = base_plugin_dir.joinpath('{}-{}'.format(docker_image.replace('/', '-'), docker_tag))

    if not base_plugin_dir.exists():
        base_plugin_dir.mkdir(parents=True, exist_ok=True)

    for name, element in plugins:
        plugin_path = os.path.join(base_plugin_dir, name)

        if not os.path.exists(plugin_path):
            os.mkdir(plugin_path)

        with open(os.path.join(plugin_path, 'plugin.xml'), 'w') as fh:
            reparsed = parseString(ET.tostring(element, 'utf-8'))
            fh.write(reparsed.toprettyxml(indent="\t"))


@cli.group()
def template():
    pass


@template.command('register')
@click.option('-c', '--config', type=str, default='full')
@click.pass_obj
def template_register(obj, config):
    """ Register a Pywr template with Hydra. """

    client = get_logged_in_client(obj)
    register_template(client, config_name=config)
    
    
@template.command('unregister')
@click.option('-c', '--config', type=str, default='full')
@click.pass_obj
def template_unregister(obj, config):
    """ Unregister a Pywr template with Hydra. """
    client = get_logged_in_client(obj)
    unregister_template(client, config_name=config)
