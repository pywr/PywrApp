from .exporter import PywrHydraExporter
import copy
from pywr.model import Model
from pywr.nodes import Node, Storage
from pywr.recorders import NumpyArrayNodeRecorder, NumpyArrayStorageRecorder
from pywr.recorders.progress import ProgressRecorder
from .template import PYWR_ARRAY_RECORDER_ATTRIBUTES


def generate_node_array_recorders(model):
    """ Helper function to add NumpyArrayXXX recorders to a Pywr model. """

    # Add node recorders
    for node in model.nodes:
        name = '__hydra_recorder__:{}'.format(node.name)
        if isinstance(node, Node):
            yield NumpyArrayNodeRecorder(model, node, name=name)
        elif isinstance(node, Storage):
            yield NumpyArrayStorageRecorder(model, node, name=name)
        else:
            import warnings
            warnings.warn('Unrecognised node subclass "{}" with name "{}". Skipping '
                          'recording this node.'.format(node.__class__.__name__, node.name),
                          RuntimeWarning)


class PywrHydraRunner(PywrHydraExporter):
    """ An extension to `PywrHydraExporter` that adds methods for running a Pywr model. """
    def __init__(self, *args, **kwargs):
        super(PywrHydraRunner, self).__init__(*args, **kwargs)
        self.model = None
        self._array_recorders = None

    def _copy_scenario(self):
        # Now construct a scenario object
        scenario = self.data.scenarios[0]
        scenario = copy.deepcopy(scenario)
        scenario.resourcescenarios = []
        return scenario

    def load_pywr_model(self):
        """ Create a Pywr model from the exported data. """
        pywr_data = self.get_pywr_data()
        model = Model.load(pywr_data)
        self.model = model

    def run_pywr_model(self):
        """ Run a Pywr model from the exported data.

        If no model has been loaded (see `load_pywr_model`) then a load is attempted.
        """
        if self.model is None:
            self.load_pywr_model()

        model = self.model

        # Add a progress recorder to monitor the run.
        ProgressRecorder(model)

        # Add recorders for monitoring the simulated timeseries of nodes
        array_recorders = []
        for recorder in generate_node_array_recorders(model):
            array_recorders.append(recorder)

        # Check the model
        model.check()

        # Force a setup regardless of whether the model has been run or setup before
        model.setup()

        # Now run the model.
        run_stats = model.run()

        # Save these for later
        self._array_recorders = array_recorders

    def _get_resource_attribute_id(self, node_name, attribute_name):

        attribute = self._get_attribute_from_name(attribute_name)
        attribute_id = attribute['id']

        for node in self.data['nodes']:
            if node['name'] == node_name:
                resource_attributes = node['attributes']
                break
        else:
            raise ValueError('Node name "{}" not found in network data.'.format(node_name))

        for resource_attribute in resource_attributes:
            if resource_attribute['attr_id'] == attribute_id:
                return resource_attribute['id']
        else:
            raise ValueError('No resource attribute for node "{}" and attribute "{}" found.'.format(node_name, attribute))

    def _get_attribute_from_name(self, name):

        for attribute_id, attribute in self.attributes.items():
            if attribute['name'] == name:
                return attribute
        raise ValueError('No attribute with name "{}" found.'.format(name))

    def save_pywr_results(self, client):
        """ Save the outputs from a Pywr model run to Hydra. """
        scenario = self._copy_scenario()

        for resource_scenario in self.generate_array_recorder_resource_scenarios():
            scenario['resourcescenarios'].append(resource_scenario)

        client.update_scenario(scenario)

    def generate_array_recorder_resource_scenarios(self):
        """ Generate resource scenario data from NumpyArrayXXX recorders. """
        if self._array_recorders is None:
            # TODO turn this on when logging is sorted out.
            # logger.info('No array recorders defined not results saved to Hydra.')
            return

        for recorder in self._array_recorders:
            df = recorder.to_dataframe()
            value = df.to_json()

            # Get the attribute and its ID
            attribute = PYWR_ARRAY_RECORDER_ATTRIBUTES[recorder.__class__]
            resource_attribute_id = self._get_resource_attribute_id(recorder.node.name, attribute)

            resource_scenario = self._make_dataset_resource_scenario(recorder.name, value, resource_attribute_id,
                                                                     encode_to_json=False, data_type='dataframe')

            yield resource_scenario


