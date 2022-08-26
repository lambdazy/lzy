#!/bin/bash

set -u

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

start

run $_f \
    pip install -r lint_requirements.txt

run $_t \
    mypy --install-types --non-interactive \
      --show-error-codes --pretty  \
      -p lzy

finish
