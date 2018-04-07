import os
import pytest


@pytest.fixture()
def model_directory():
    return os.path.join(os.path.dirname(__file__), 'models')


@pytest.fixture()
def simple1(model_directory):
    return os.path.join(model_directory, 'simple1.json')


@pytest.fixture(params=[
    'simple1.json',
    'reservoir2.json',
    'parameter_reference.json',
])
def pywr_json_filename(request, model_directory):
    return os.path.join(model_directory, request.param)