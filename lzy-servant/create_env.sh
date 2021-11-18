#!/bin/bash

name="$1"
version="$2"
lzy_py_path="$3"
eval "$(conda shell.bash hook)" && \
  conda create --name "$name" "python=$version" pip && \
  conda activate "$name" && \
  python -m ensurepip && \
  pip install -r requirements.txt && \
  cd "$lzy_py_path" && pip install .
