#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash

set -u

source ./util.sh

start

run $_f "black lzy/ tests/ examples/ setup.py"
run $_f "isort lzy/ tests/ examples/ setup.py"
run $_t "mypy --install-types --non-interactive --check-untyped-defs --show-error-codes --pretty --strict -p ai.lzy.v1 -p lzy"
run $_t "pyright --stats --lib"

finish
