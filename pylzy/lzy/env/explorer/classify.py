from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import FrozenSet, Set, Dict, cast, Iterable, Tuple
from types import ModuleType

from importlib.machinery import ExtensionFileLoader
from importlib_metadata import Distribution

from lzy.utils.pypi import check_package_version_exists

from .search import ModulesSet
from .packages import (
    LocalPackage,
    LocalDistribution,
    PypiDistribution,
    BrokenModules,
    BasePackage,
)
from .utils import (
    get_files_to_distributions,
    get_stdlib_module_names,
    get_builtin_module_names,
    check_url_is_local_file,
)


DistributionSet = Set[Distribution]


class ModuleClassifier:
    def __init__(self, pypi_index_url: str):
        self.pypi_index_url = pypi_index_url

        self.stdlib_module_names = get_stdlib_module_names()
        self.builtin_module_names = get_builtin_module_names()
        self.files_to_distributions = get_files_to_distributions()

    def classify(self, modules: Iterable[ModuleType]) -> FrozenSet[BasePackage]:
        distributions: DistributionSet = set()
        binary_distributions: DistributionSet = set()
        modules_without_distribution: ModulesSet = set()

        self._classify_modules(
            modules,
            distributions,
            binary_distributions,
            modules_without_distribution
        )
        packages = self._classify_distributions(distributions, binary_distributions)
        packages |= self._classify_modules_without_distributions(modules_without_distribution)

        return frozenset(packages)

    def _classify_modules(
        self,
        modules: Iterable[ModuleType],
        distributions: DistributionSet,
        binary_distributions: DistributionSet,
        modules_without_distribution: ModulesSet,
    ) -> None:
        """
        Here we are dividing modules into two piles:
        those which are part of some distribution and those which are not.
        Also here we are noting distributions with binary modules.
        """
        for module in modules:
            module_name = module.__name__
            top_level: str = module_name.split('.')[0]
            filename = getattr(module, '__file__', None)

            # Modules without __file__ doesn't represent specific file at the disk
            # so we a generally doesn't interested about it.
            # It can be namespace modules or strange virtual modules as the `six.moves.*`.
            # It's totally okay that we are skipping it and don't require any warning,
            # because we should process such distributions by other signes.
            if not filename:
                continue

            # We also doesn't interested in standard modules
            if (
                top_level in self.stdlib_module_names or
                top_level in self.builtin_module_names
            ):
                continue

            distribution = self.files_to_distributions.get(filename)
            if distribution and not self._check_distribution_is_editable(distribution):
                distributions.add(distribution)

                if self._check_module_is_binary(module):
                    binary_distributions.add(distribution)

                continue

            modules_without_distribution.add(module)

    def _classify_distributions(
        self,
        distributions: DistributionSet,
        binary_distributions: DistributionSet,
    ) -> Set[BasePackage]:
        """
        Here we are dividing distributions into two piles:
        those which are present on pypi and thos which is not.
        """
        result: Set[BasePackage] = set()

        for distribution in distributions:
            package: BasePackage
            # TODO: make this check parallel
            if self._check_distribution_at_pypi(
                pypi_index_url=self.pypi_index_url,
                name=distribution.name,
                version=distribution.version,
            ):
                package = PypiDistribution(
                    name=distribution.name,
                    version=distribution.version,
                    pypi_index_url=self.pypi_index_url,
                )
            else:
                paths, bad_paths = self._get_distribution_paths(distribution)
                is_binary = distribution in binary_distributions
                package = LocalDistribution(
                    name=distribution.name,
                    version=distribution.version,
                    paths=paths,
                    is_binary=is_binary,
                    bad_paths=bad_paths,
                )

            result.add(package)

        return result

    def _classify_modules_without_distributions(
        self,
        modules_without_distribution: ModulesSet
    ) -> Set[BasePackage]:
        """
        Based on module names and paths here we are creating
        "virtual" distributions which are consists of local files.
        """

        fake_distributions: Dict[str, Set[str]] = {}
        binary_distributions: Set[str] = set()
        broken: Dict[str, str] = {}

        for module in modules_without_distribution:
            module_name = module.__name__
            top_level: str = module_name.split('.')[0]

            path = self._get_top_level_path(module)

            if not path:
                broken[module_name] = cast(str, module.__file__)
                continue

            fake_distributions.setdefault(top_level, set())
            fake_distributions[top_level].add(path)

            if self._check_module_is_binary(module):
                binary_distributions.add(top_level)

        result: Set[BasePackage] = set()
        package: BasePackage
        if broken:
            package = BrokenModules(name='packages_with_bad_path', modules_paths=broken)
            result.add(package)

        for top_level, paths in fake_distributions.items():
            package = LocalPackage(
                name=top_level,
                paths=frozenset(paths),
                is_binary=top_level in binary_distributions
            )
            result.add(package)

        return result

    def _check_distribution_is_editable(self, distribution: Distribution) -> bool:
        """Here we checking if package installed as editable installation.

        Relevant links:
        https://github.com/python/importlib_metadata/issues/404 discussion
        https://packaging.python.org/en/latest/specifications/direct-url/
        https://github.com/conda/conda/issues/11580
        """
        direct_url_str = distribution.read_text('direct_url.json')
        if not direct_url_str:
            # there is not direct_url.json
            return False

        direct_url_data = json.loads(direct_url_str)

        url = direct_url_data.get('url')
        if not url:
            # just in case, because spec tells that url must be
            # always present
            return False

        editable = direct_url_data.get('dir_info', {}).get('editable')

        # The whole thing about direct_url.json is that
        # it is a sign of editable installation from the one hand,
        # but from the other hand, conda left this file at it's
        # distributions as a artifact of repack process
        # (see https://github.com/conda/conda/issues/11580).
        # In case of conda, there will be some strange path like
        # file:///work/ci_py311/idna_1676822698822/work
        # which is probably will not exists at user's system
        return editable and check_url_is_local_file(url)

    @staticmethod
    @lru_cache(maxsize=None)
    def _check_distribution_at_pypi(pypi_index_url: str, name: str, version: str) -> bool:
        """
        Just cached versioni of `check_package_version_exists`, but it can be (and would be)
        overrided in descendant classes.
        """

        return check_package_version_exists(
            pypi_index_url=pypi_index_url,
            name=name,
            version=version,
        )

    def _get_distribution_paths(
        self, distribution: Distribution
    ) -> Tuple[FrozenSet[str], FrozenSet[str]]:
        """
        If Distribution files are foo/bar, foo/baz and foo1,
        we want to return {<site-packages>/foo, <site-packages>/foo1}
        """

        paths = set()
        bad_paths = set()

        base_path = distribution.locate_file('')

        for path in distribution.files:
            abs_path = distribution.locate_file(path).resolve()
            if base_path not in abs_path.parents:
                bad_paths.add(str(abs_path))
                continue

            rel_path = abs_path.relative_to(base_path)
            first_part = rel_path.parts[0]
            path = base_path / first_part
            paths.add(str(path))

        return frozenset(paths), frozenset(bad_paths)

    def _check_module_is_binary(self, module: ModuleType) -> bool:
        loader = getattr(module, '__loader__', None)
        return bool(loader and isinstance(loader, ExtensionFileLoader))

    def _get_top_level_path(self, module: ModuleType) -> str:
        """
        Get path of module's top-level dir.
        Why not just `top_level_module.__file__`?
        Catch is about namespace packages: modules with same top-level name may
        have different top-level paths.
        """

        name = module.__name__
        package = module.__package__
        filename = cast(str, module.__file__)

        # Module without a __package__ represents top-level .py file,
        # like site-packages/typing_extensions.py
        # Relevant docs: https://docs.python.org/3/reference/import.html#__package__
        if not package:
            return filename

        level = len(name.split('.'))  # foo.bar.baz is level 3

        # 1) if module is a package, its filename contain /__init__.py
        # 2) if module is not a package, its __name__ != __package__
        if name != package:
            level -= 1

        filename_parts = filename.split(os.sep)

        # 1) so if foo.bar.baz is a package, it have path a/b/c/foo/bar/baz/__init__.py
        # and when we doing [:-3], we truncating three parts from it and getting a/b/c/foo.
        # 2) if boo.bar.baz is a module, it have path a/b/c/foo/bar/baz.py,
        # we are doing level -= 1 and truncating two parts and getting a/b/c/foo
        top_level_parts = filename_parts[:-level]

        return os.sep.join(top_level_parts)
