from __future__ import annotations

from dataclasses import dataclass
from typing import List, FrozenSet, Type, TypeVar

from lzy.env.python.base import PackagesDict

from .base import BaseExplorer, ModulePathsList, PackagesDict
from .classify import ModuleClassifier
from .search import VarsNamespace, get_transitive_namespace_dependencies
from .packages import (
    BrokenModules,
    LocalPackage,
    BasePackage,
    PypiDistribution,
    LocalDistribution
)


P = TypeVar('P', bound=BasePackage)


@dataclass
class AutoExplorer(BaseExplorer):
    pypi_index_url: str
    additional_pypi_packages: PackagesDict

    def get_local_module_paths(self, namespace: VarsNamespace) -> ModulePathsList:
        packages = self._get_packages(namespace, LocalPackage)

        filtered: List[LocalPackage] = []
        binary: List[LocalPackage] = []
        nonbinary: List[LocalPackage] = []

        for package in packages:
            if package.name in self.additional_pypi_packages:
                array = filtered
            elif package.is_binary:
                array = binary
            else:
                array = nonbinary

            array.append(package)

        packages_with_bad_paths = [
            p for p in nonbinary
            if isinstance(p, LocalDistribution) and p.bad_paths
        ]

        if filtered:
            self.log.debug(
                "Some dependency packages were classified as local but filtered due "
                "to explicit value of additional_pypi_packages: %s",
                filtered
            )

        if binary:
            self.log.warning(
                "Some dependency packages were classified as local but they "
                "contain a binary files; these packages wouldn't be transferred to a remote "
                "host. If you need these packages, specify it explicitly "
                "at additional_pypi_packages to force-classify it as pypi packages: %s",
                binary,
            )

        if packages_with_bad_paths:
            self.log.warning(
                "Some dependency packages were classified as local but they "
                "contain files with non-standard paths; these paths wouldn't be transferred to "
                "a remote host, but packages will be transferred without it. "
                "In case of any troubles with these packages, specify it explicitly "
                "at additional_pypi_packages to force-classify it as pypi packages: %s",
                packages_with_bad_paths
            )

        if nonbinary:
            self.log.debug(
                "Next dependency packages were classified as local packages "
                "and will be transfered to a remote host: %s",
                nonbinary
            )

        return list(set().union(*(p.paths for p in nonbinary)))

    def get_pypi_packages(self, namespace: VarsNamespace) -> PackagesDict:
        packages = self._get_packages(namespace, PypiDistribution)

        overrided: List[PypiDistribution] = [
            p for p in packages if p.name in self.additional_pypi_packages
        ]

        if overrided:
            self.log.debug(
                "Next dependency packages were classified as pypi packages "
                "but were overrided by additional_pypi_packages option: %s",
                overrided
            )

        return {
            **{p.name: p.version for p in packages},
            **self.additional_pypi_packages
        }

    def _get_packages(
        self,
        namespace: VarsNamespace,
        filter_class: Type[P],
    ) -> FrozenSet[P]:
        modules = get_transitive_namespace_dependencies(namespace)

        classifier = ModuleClassifier(self.pypi_index_url)

        packages = classifier.classify(modules)
        broken = [p for p in packages if isinstance(p, BrokenModules)]
        if broken:
            self.log.warning(
                'while exploring local environment we failed to classify some modules '
                'so these moduels will be omitted: %s', broken
            )

        return frozenset(p for p in packages if isinstance(p, filter_class))
