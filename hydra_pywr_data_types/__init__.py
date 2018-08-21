from hydra_base.lib.HydraTypes.Types import DataType, Descriptor


class PywrParameter(Descriptor):
    tag = 'PYWR_PARAMETER'

class PywrRecorder(Descriptor):
    tag = 'PYWR_RECORDER'


PYWR_DATA_TYPE_MAP = {
    'parameters': PywrParameter,
    'recorders': PywrRecorder
}
