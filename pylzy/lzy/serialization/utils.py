from typing import Dict

import pkg_resources


def all_installed_packages() -> Dict[str, str]:
    return {
        entry.project_name: entry.version
        # working_set is actually iterable see sources
        for entry in pkg_resources.working_set  # pylint: disable=not-an-iterable
    }


cached_installed_packages = all_installed_packages()
