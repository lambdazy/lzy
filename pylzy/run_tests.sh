#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

set -ex

coverage run --source=./lzy -m unittest discover ./tests >test_output 2>&1
coverage report
coverage-badge -o coverage.svg -f
