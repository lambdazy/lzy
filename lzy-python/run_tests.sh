#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lzy -i bash

export TWINE_USERNAME="__token__"
# export TWINE_PASSWORD="<provide token here>"
python -m unittest discover -s ./tests | tee ./test_output
