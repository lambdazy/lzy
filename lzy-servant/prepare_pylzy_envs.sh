#!/bin/bash

create_env_safely() {
    lzy_py_path="$3"
    cd "$lzy_py_path"
    create_env "$1" "2"
    cd -
}

create_env() {
  name="$1"
  version="$2"
  eval "$(conda shell.bash hook)" && \
    conda create --name "$name" "python=$version" pip && \
    conda activate "$name" && \
    python -m ensurepip && \
    # probably it's not needed to install requirements
    pip install -r requirements.txt && \
    pip install . && \
    return 0
}

lzy_py_path="$1"
create_env_safely "default" "3.9.7" "$lzy_py_path" \
    && create_env_safely "py37" "3.7.11" "$lzy_py_path" \
    && create_env_safely "py38" "3.8.12" "$lzy_py_path" \
    && create_env_safely "py39" "3.9.7" "$lzy_py_path" \
#    && create_env_safely "py310" "3.10.0" "$lzy_py_path"
