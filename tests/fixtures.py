import os
import pytest


@pytest.fixture()
def model_directory():
    return os.path.join(os.path.dirname(__file__), 'models')


@pytest.fixture()
def simple1(model_directory):
    return os.path.join(model_directory, 'simple1.json')