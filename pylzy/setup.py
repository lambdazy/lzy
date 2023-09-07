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
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
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
        python_requires=">=3.8",
        packages=setuptools.find_packages(include=('ai*', 'lzy*')),
        long_description_content_type='text/markdown',
        long_description=read_readme()
    )
