#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

python -m unittest discover -s ./tests/v2 | tee ./test_output
