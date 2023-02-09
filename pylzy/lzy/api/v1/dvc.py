import sys
from collections import namedtuple
import os
from typing import Optional

from dvc.repo import Repo
import yaml

from lzy.logs.config import get_logger
from lzy.types import File
from . import LzyWorkflow

_LOG = get_logger(__name__)

dvc_file_name = 'dvc.yaml'
dvc_lock_file_name = 'dvc.lock'
params_file_name = 'params.yaml'
requirements_file_name = 'dvc_requirements.txt'

# which can be places in YAML
primitive_types = (int, float, bool, str)

ArgInfo = namedtuple('ArgInfo', ('name', 'type', 'value', 'entry_id'))


def generate_dvc_files(wf: LzyWorkflow) -> None:
    libs = [f'{name}=={version}' for name, version in wf.auto_py_env.libraries.items()]
    with open(requirements_file_name, 'w') as f:
        f.write('\n'.join(libs))

    cwd = os.getcwd()

    deps = [requirements_file_name] + [
        rel_path for rel_path in [
            _get_relative_path(cwd, module_path) for module_path in wf.auto_py_env.local_modules_path
        ]
        if rel_path
    ]

    args = []
    intermediate_entry_ids = set()
    for call in wf.call_list:
        func = call.signature.func

        for i, (arg_value, arg_entry_id) in enumerate(zip(call.args, call.arg_entry_ids)):
            arg_name = func.arg_names[i]
            arg_type = func.input_types[arg_name]
            arg_info = ArgInfo(name=arg_name, type=arg_type, value=arg_value, entry_id=arg_entry_id)
            args.append(arg_info)

        for arg_name, arg_value in call.kwargs.items():
            arg_type = func.input_types[arg_name]
            arg_entry_id = call.kwarg_entry_ids[arg_name]
            arg_info = ArgInfo(name=arg_name, type=arg_type, value=arg_value, entry_id=arg_entry_id)
            args.append(arg_info)

        intermediate_entry_ids = intermediate_entry_ids.union(set(call.entry_ids))

    received_entry_ids = set()
    params = []
    input_file_paths = []
    for arg_info in args:
        if arg_info.entry_id in intermediate_entry_ids or arg_info.entry_id in received_entry_ids:
            continue

        received_entry_ids.add(arg_info.entry_id)

        if arg_info.type is File:
            input_file_paths.append(str(arg_info.value.path))
        elif arg_info.type in primitive_types:
            arg_info = ArgInfo(
                name=arg_info.name,
                type=arg_info.type,
                value=arg_info.value,
                entry_id=arg_info.entry_id,
            )
            params.append(arg_info)  # TODO: name collisions
        else:
            raise ValueError('you can use only File or primitive types in @op\'s using DVC')

    params_yaml = {param.name: param.value for param in params}

    with open(params_file_name, 'w') as f:
        f.write(yaml.dump(params_yaml))

    repo = Repo()

    stage = repo.stage.add(
        name='main',
        cmd=' '.join(['python'] + sys.argv),  # TODO: think about stable cmd
        deps=deps + input_file_paths,
        params=[param.name for param in params],
        force=True,
    )
    stage.save()  # will calculate file hashes
    stage.dump(update_lock=True)

    # TODO: whiteboard output


def _get_relative_path(cwd: str, module_path: str) -> Optional[str]:
    if not module_path.startswith(cwd):
        _LOG.warning(f'skipping local module "{module_path}" - it is not in the cwd and cannot be tracked by DVC')
        return None

    path = module_path[len(cwd) + 1:]

    if len(path) == 0:
        _LOG.warning(f'skipping local module "{module_path} - it is matched cwd')
        return None

    return path
