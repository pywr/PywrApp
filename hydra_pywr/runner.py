from .exporter import PywrHydraExporter
import copy
from pywr.model import Model
from pywr.recorders.progress import ProgressRecorder


class PywrHydraRunner(PywrHydraExporter):
    """ An extension to `PywrHydraExporter` that adds methods for running a Pywr model. """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = None

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
        # Force a setup regardless of whether the model has been run or setup before
        model.setup()

        # Add a progress recorder to monitor the run.
        ProgressRecorder(model)

        # Check the model
        model.check()

        # Now run the model.
        run_stats = model.run()

    def save_pywr_results(self):
        """ Save the outputs from a Pywr model run to Hydra. """

        scenario = self._copy_scenario()
