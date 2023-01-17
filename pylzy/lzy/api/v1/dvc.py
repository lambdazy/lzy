import sys
from collections import namedtuple
import os

from lzy.types import File
from . import LzyWorkflow

requirements_file_name = 'dvc_requirements.txt'
dvc_file_name = 'dvc.yaml'
params_file_name = 'params.yaml'

# which can be places in YAML
primitive_types = (int, float, bool, str)

ArgInfo = namedtuple('ArgInfo', ('name', 'type', 'value', 'entry_id'))


def generate_dvc_files(wf: LzyWorkflow) -> None:
    libs = [f'{name}=={version}' for name, version in wf.auto_py_env.libraries.items()]
    with open(requirements_file_name, 'w') as f:
        f.write('\n'.join(libs))

    cwd = os.getcwd()

    deps = [requirements_file_name] + [
        _get_relative_path(cwd, module_path) for module_path in wf.auto_py_env.local_modules_path
    ]

    args = []
    intermediate_entry_ids = set()
    for call in wf.owner.runtime.calls:
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

    entry_id_to_first_occurred_name = {}
    params = []
    input_file_paths = []
    for arg_info in args:
        if arg_info.entry_id in intermediate_entry_ids:
            continue

        if arg_info.entry_id not in entry_id_to_first_occurred_name:
            entry_id_to_first_occurred_name[arg_info.entry_id] = arg_info.name

        if arg_info.type is File:
            input_file_paths.append(str(arg_info.value.path))
        elif arg_info.type in primitive_types:
            arg_info = ArgInfo(
                name=entry_id_to_first_occurred_name[arg_info.entry_id],
                type=arg_info.type,
                value=arg_info.value,
                entry_id=arg_info.entry_id,
            )
            params.append(arg_info)  # TODO: name collisions
        else:
            raise ValueError('you can use only File or primitive types in @op\'s using DVC')

    dvc_yaml = {
        'stages': {
            'main': {
                'cmd': ' '.join(sys.argv + input_file_paths),  # TODO: think about stable cmd
                'deps': deps + input_file_paths,
                'params': [param.name for param in params],
            }
        }
    }
    params_yaml = {param.name: param.value for param in params}

    import yaml
    with open(dvc_file_name, 'w') as f:
        f.write(yaml.dump(dvc_yaml))
    with open(params_file_name, 'w') as f:
        f.write(yaml.dump(params_yaml))

    # TODO: whiteboard output


def _get_relative_path(cwd: str, module_path: str) -> str:
    if not module_path.startswith(cwd):
        raise RuntimeError(f'local module path "{module_path}" must start with working directory path "{cwd}"')
    return module_path[len(cwd) + 1:]
