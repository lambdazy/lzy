#!/usr/bin/env nix-shell
#! nix-shell ../pylzy/build.nix -A shell-lint -i bash

set -u

source ../pylzy/util.sh

start

# run $_f "black lzy/"
# run $_f "isort lzy/"
run $_t "mypy --install-types --non-interactive --check-untyped-defs --show-error-codes --pretty -p lzy"
# run $_t pyright

finish
