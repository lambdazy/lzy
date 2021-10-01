import setuptools

setuptools.setup(
    name='lzy-py',
    version='0.0.1',
    package_dir={'': 'lzy'},
    packages=['api', 'api/whiteboard', 'api/_proxy'],
    install_requires=[
        'cloudpickle==2.0.0'
    ],
    python_requires='>=3.7'
)
