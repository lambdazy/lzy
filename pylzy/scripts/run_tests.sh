#!/bin/bash

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

pip install catboost coverage coverage-badge
pip install -r requirements.txt

start

run $_c \
    coverage run -a --source=./lzy -m unittest discover ./tests

run $_c \
    coverage report

run $_c \
    coverage-badge -o coverage.svg -f

finish
