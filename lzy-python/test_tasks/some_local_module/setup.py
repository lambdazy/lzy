from setuptools import setup, find_packages

setup(
    name='some_local_module',
    version='1.0.0',
    packages=['src'],  # Required
    python_requires='>=3.6, <4'
)
