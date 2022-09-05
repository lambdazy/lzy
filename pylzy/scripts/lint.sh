#!/usr/bin/env nix-shell
#! nix-shell build.nix -A lint -i bash

set -u

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

pip install -r lint_requirements.txt

start

run $_f \
    black lzy/ tests/ examples/ setup.py

run $_f \
    isort lzy/ tests/ examples/ setup.py

run $_t \
    mypy --install-types --non-interactive \
      --show-error-codes --pretty  \
      -p lzy

finish
