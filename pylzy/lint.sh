#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash


echo "Calling mypy typechecking"

mypy .
