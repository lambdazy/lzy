import setuptools

setuptools.setup(
    name='lzy-py',
    version='0.0.1',
    package_dir={'': 'src/main/python'},
    packages=['lzy'],
    install_requires=[
        'cloudpickle==2.0.0'
    ],
    python_requires='>=3.7'
)
