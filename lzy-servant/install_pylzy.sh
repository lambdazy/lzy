#!/bin/bash

conda_env="$1"
lzy_py_path="$2"

eval "$(conda shell.bash hook)" && \
  conda activate "$conda_env" && \
  cd "$lzy_py_path" && pip install .