import importlib


def test_runtime_package_imports():
    module = importlib.import_module("iii_drone_runtime")

    assert module.__version__ == "0.1.0"
