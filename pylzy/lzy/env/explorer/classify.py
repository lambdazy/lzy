from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import FrozenSet, Set, Dict, cast, Iterable, Tuple, Union, List
from types import ModuleType

from importlib.machinery import ExtensionFileLoader
from importlib_metadata import Distribution

from typing_extensions import assert_never
from packaging.tags import PythonVersion

from lzy.utils.pypi import (
    check_package_version_exists,
    check_package_version_exists_on_target_platform,
)

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
    get_names_to_distributions,
    get_stdlib_module_names,
    get_builtin_module_names,
    get_requirements_to_meta_packages,
    get_name_from_requirement_string,
    check_distribution_is_meta_package,
    check_url_is_local_file,
    is_wellknown_fake_module,
)


DistributionSet = Set[Distribution]


class ModuleClassifier:
    def __init__(self, pypi_index_url: str, target_python: PythonVersion):
        self.pypi_index_url = pypi_index_url
        self.target_python = target_python

        self.stdlib_module_names = get_stdlib_module_names()
        self.builtin_module_names = get_builtin_module_names()
        self.files_to_distributions = get_files_to_distributions()
        self.names_to_distributions = get_names_to_distributions()
        self.requirements_to_meta_packages = get_requirements_to_meta_packages()

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
        packages |= self._process_meta_package_distributions(packages, binary_distributions)
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
        Also, here we are noting distributions with binary modules.
        """
        for module in modules:
            module_name = module.__name__
            top_level: str = module_name.split('.')[0]
            filename = getattr(module, '__file__', None)

            # Modules without __file__ doesn't represent specific file at the disk,
            # so we are generally not interested about it.
            # It can be namespace modules or strange virtual modules as the `six.moves.*`.
            # It's totally okay that we are skipping it and don't require any warning,
            # because we should process such distributions by other properties.
            if not filename:
                continue

            # We also not interested in standard modules
            if (
                top_level in self.stdlib_module_names or
                top_level in self.builtin_module_names
            ):
                continue

            # ... and fake modules
            if is_wellknown_fake_module(top_level, filename):
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

        # sorting needed for tests repeatability
        return {
            self._classify_distribution(distribution, binary_distributions)
            for distribution in sorted(distributions, key=lambda d: (d.name, d.version))
        }

    def _classify_distribution(
        self,
        distribution: Distribution,
        binary_distributions: DistributionSet,
    ) -> Union[PypiDistribution, LocalDistribution]:
        # TODO: make this check parallel
        if self._check_distribution_at_pypi(
            pypi_index_url=self.pypi_index_url,
            name=distribution.name,
            version=distribution.version,
        ):
            have_server_supported_tags = self._check_distribution_platform_at_pypi(
                pypi_index_url=self.pypi_index_url,
                name=distribution.name,
                version=distribution.version,
                target_python=self.target_python
            )

            return PypiDistribution(
                name=distribution.name,
                version=distribution.version,
                pypi_index_url=self.pypi_index_url,
                have_server_supported_tags=have_server_supported_tags,
            )

        paths, bad_paths = self._get_distribution_paths(distribution)
        is_binary = distribution in binary_distributions
        return LocalDistribution(
            name=distribution.name,
            version=distribution.version,
            paths=paths,
            is_binary=is_binary,
            bad_paths=bad_paths,
        )

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

    def _process_meta_package_distributions(
        self,
        packages: Set[BasePackage],
        binary_distributions: DistributionSet,
    ) -> Set[BasePackage]:
        """
        Here we are trying to find and classify meta packages.
        We are considering as meta package any package that doesn't contain any files
        except for its meta.
        Problem is, these meta-packages can't be found by usual module exploring because there no
        modules in it.

        For example, meta package "tensorflow" requires usual package "tensorflow-intel".
        We must find "tensorflow" in our exploring process to send it as requirement to server,
        because server could have different platform from local machine and requirements
        from meta package "tensorflow" will be compiled to another packages list.
        (Also we will filter "tensorflow-intel" from requirements because server have
        wrong platorm for it, look at `_check_distribution_platform_at_pypi`)

        """

        result: Set[BasePackage] = set()
        meta_packages: DistributionSet = set()
        seen_meta_packages: Set[str] = set()

        def add_meta_requirements(name: str) -> None:
            """here we check if package with 'name' are required by one or more
            meta-packages and if it is, we adding these meta packages to our DFS"""

            new_meta_packages = self.requirements_to_meta_packages.get(name, ())
            for meta_package in new_meta_packages:
                if meta_package.name not in seen_meta_packages:
                    meta_packages.add(meta_package)

        # step 1: finding all meta packages that requires any package
        # we already found through module exploring
        for package in packages:
            add_meta_requirements(package.name)

        # step 2: classify all found meta packages
        while meta_packages:
            meta_package = meta_packages.pop()
            seen_meta_packages.add(meta_package.name)

            package = self._classify_distribution(meta_package, set())

            # step 3: if metapackage is found on pypi, just add it to result
            # as usual PypiPackage
            if isinstance(package, PypiDistribution):
                result.add(package)
                continue

            # package var must include PypiDistribution | LocalDistribution
            # so this will never assert
            if not isinstance(package, LocalDistribution):
                assert_never()

            # XXX: All code below is needed for theoretical
            # edge cases and could be safely removed:
            # if we have "local" meta-package (which was failed to find at pypi)
            # and it depends on different meta-packages, for example

            # step 4: if metapackage is not found on pypi,
            # we must treat it as local distribution,
            # but it lacking files by defenition.
            # also, as it lacking files, we didn't found
            # it's requirement packages through module exporing
            requirements: List[str] = meta_package.requires or []
            for requirement_string in requirements:
                requirement_name = get_name_from_requirement_string(requirement_string)

                # we failed to parse this string
                if not requirement_name:
                    continue

                distribution = self.names_to_distributions.get(requirement_name)

                # it is maybe okay, if we fail to find requirement package on local machine:
                # 1) we ignoring markers in requirement_string, for example
                #    "package<1.0.0; python_version<'3.10'"
                #    so this requirement may be not real for us right here.
                # 2) we could fail to find it by another reason, for example,
                #    we are ignoring packages, builded with
                #    obsolete egg-info; I think it is too much stars to align.
                if not distribution:
                    continue

                # if requirement is another meta package, we need to add it to our DFS
                if check_distribution_is_meta_package(distribution):
                    if distribution.name not in seen_meta_packages:
                        meta_packages.add(distribution)

                    continue

                # it is non-meta package, so classify it as usual
                package = self._classify_distribution(distribution, binary_distributions)
                result.add(package)

                # if requirement is a non-meta package, it still may be
                # required by another meta package
                add_meta_requirements(package.name)

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
        Just cached version of `check_package_version_exists`, but it can be (and would be)
        overrided in descendant classes.
        """

        return check_package_version_exists(
            pypi_index_url=pypi_index_url,
            name=name,
            version=version,
        )

    @staticmethod
    @lru_cache(maxsize=None)
    def _check_distribution_platform_at_pypi(
        pypi_index_url: str,
        name: str,
        version: str,
        target_python: PythonVersion
    ) -> bool:
        return check_package_version_exists_on_target_platform(
            pypi_index_url=pypi_index_url,
            name=name,
            version=version,
            target_python=target_python,
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

        for path in distribution.files or ():
            abs_path = distribution.locate_file(path).resolve()
            if base_path not in abs_path.parents:
                bad_paths.add(str(abs_path))
                continue

            rel_path = abs_path.relative_to(base_path)
            first_part = rel_path.parts[0]
            result_path = base_path / first_part
            paths.add(str(result_path))

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
