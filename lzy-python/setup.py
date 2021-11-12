import setuptools

from installer import Installer

setuptools.setup(
    name='lzy-py',
    version='0.0.1',
    package_dir={'': '.'},
    packages=['lzy', 'lzy/api', 'lzy/api/whiteboard', 'lzy/api/_proxy',
              'lzy/model', 'lzy/servant', 'lzy/api/pkg_info'],
    install_requires=[
        'cloudpickle==2.0.0',
        'pyyaml'

    ],
    python_requires='>=3.7',
    cmdclass={
        'install': Installer
    }
)
