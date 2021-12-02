#!/bin/bash

lzy_py_path="$1"
./install_pylzy.sh "default" "$lzy_py_path" \
    && ./install_pylzy.sh "py37" "$lzy_py_path" \
    && ./install_pylzy.sh "py38" "$lzy_py_path" \
    && ./install_pylzy.sh "py39" "$lzy_py_path" \
#    && ./create_env.sh "py310" "3.10.0"
