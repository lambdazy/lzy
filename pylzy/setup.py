from typing import List

import setuptools


def read_readme() -> str:
    with open("readme.md", "r") as file:
        return file.read()


def read_version(path="lzy/version/version"):
    with open(path) as file:
        return file.read().rstrip()


def read_requirements() -> List[str]:
    requirements = []
    with open("requirements.txt", "r") as file:
        for line in file:
            requirements.append(line.rstrip())
    return requirements


setuptools.setup(
    name="pylzy",
    version=read_version(),
    license="LICENSE",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10"
    ],
    author="ʎzy developers",
    include_package_data=True,
    package_data={
        "ai/lzy/v1": [
            "**/*.pyi",
            "*.pyi",
        ],
        "lzy": ["version/version", "logs/logging.yml"],
    },
    install_requires=read_requirements(),
    packages=[
        "lzy",
        "lzy/api",
        "lzy/api/v1",
        "lzy/api/v1/local",
        "lzy/api/v1/utils",
        "lzy/api/v1/remote",
        "lzy/api/v1/remote/model",
        "lzy/api/v1/remote/model/converter",
        "lzy/proxy",
        "lzy/py_env",
        "lzy/storage",
        "lzy/storage/async_",
        "lzy/serialization",
        "lzy/injections",
        "lzy/whiteboards",
        "lzy/utils",
        "lzy/logs",
        "ai/",
        "ai/lzy",
        "ai/lzy/v1",
        "ai/lzy/v1/common",
        "ai/lzy/v1/validation",
        "ai/lzy/v1/long_running",
        "ai/lzy/v1/whiteboard",
        "ai/lzy/v1/workflow",
        "google/protobuf",
        "google/rpc"
    ],
    python_requires=">=3.7",
    long_description_content_type='text/markdown',
    long_description=read_readme()
)
