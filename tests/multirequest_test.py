import pytest
from multinet.multirequest import Multirequest


@pytest.fixture(scope="function")
def mr():
    return Multirequest()


def test_get(mr):
    data = mr.get(("simple.test", "sinM"))
    assert isinstance(data["simple.test", "sinM"], float)
