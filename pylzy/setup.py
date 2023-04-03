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


if __name__ == '__main__':
    setuptools.setup(
        name="pylzy",
        version=read_version(),
        license="Apache-2.0",
        classifiers=[
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "License :: OSI Approved :: Apache Software License",
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
        packages=setuptools.find_namespace_packages(include=('ai*', 'google*', 'lzy*')),
        python_requires=">=3.7",
        long_description_content_type='text/markdown',
        long_description=read_readme()
    )
