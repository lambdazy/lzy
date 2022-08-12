#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash

set -u

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

start

run $_f \
    black lzy/ tests/ examples/ setup.py

run $_f \
    isort lzy/ tests/ examples/ setup.py

run $_t \
    mypy --install-types --non-interactive --check-untyped-defs \
      --show-error-codes --pretty  \
      -p ai.lzy.v1 -p lzy
      # --strict \

# run $_t pyright --stats --lib

finish
