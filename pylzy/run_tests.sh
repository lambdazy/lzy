#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

python -m unittest discover -s ./tests | tee ./test_output
