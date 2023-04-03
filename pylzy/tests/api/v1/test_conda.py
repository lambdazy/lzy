import pytest
from unittest import TestCase

from lzy.api.v1.utils.conda import generate_conda_yaml


class CondaTests(TestCase):
    def test_conda_generate_in_cache(self):
        yaml = generate_conda_yaml("3.7.11", {"pylzy": "0.0.0"})
        self.assertIn("name: py37", yaml)
        self.assertIn("pylzy==0.0.0", yaml)

    def test_conda_generate_not_in_cache(self):
        with pytest.warns(Warning, match=r"Installed python version.*"):
            yaml = generate_conda_yaml("3.7.9999", {"pylzy": "0.0.0"})
        self.assertIn("name: default", yaml)
        self.assertIn("pylzy==0.0.0", yaml)
