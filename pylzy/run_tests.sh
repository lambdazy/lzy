#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

source ./util.sh

start

run $_c "coverage run --source=./lzy -m unittest discover ./tests -a"
run $_c "coverage report"
run $_c "coverage-badge -o coverage.svg -f"

finish
