import os as _os
import typing as _typing

__all__ = [
    'SKIP_PYPI_VALIDATION',
    'skip_pypi_validation',
]


def _parse_environ_bool(key: str, default: bool) -> bool:
    true_values = {'1', 'yes', 'true', 'on'}
    false_values = {'0', 'no', 'false', 'off'}

    value: _typing.Optional[str] = _os.environ.get(key)

    if value is None:
        return default

    value = value.lower()

    if value in true_values:
        return True

    if value in false_values:
        return False

    raise TypeError(
        f'failed to parse env variable {key}; '
        f'allowed values: {", ".join(sorted(true_values | false_values))}'
    )


SKIP_PYPI_VALIDATION = 'LZY_SKIP_PYPI_VALIDATION'
skip_pypi_validation = _parse_environ_bool(SKIP_PYPI_VALIDATION, False)
