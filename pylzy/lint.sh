#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash

set -u

source ./util.sh

run $_f "black lzy/ tests/ examples/"
run $_f "isort lzy/ tests/ examples/"
run $_t "mypy --install-types --non-interactive --check-untyped-defs --show-error-codes --pretty -p ai.lzy.v1 -p lzy"
# run $_t pyright

print_cmd_exit $rc "Whole pipeline"

exit $rc
