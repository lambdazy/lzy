#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

coverage run --source=./lzy -m unittest discover ./tests | tee ./test_output
coverage report
coverage-badge -o coverage.svg -f