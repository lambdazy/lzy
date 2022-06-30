#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-publish -i bash


echo "Calling publish commands"

export TWINE_USERNAME="__token__"
# **token has to be provided by person or bot who runs the script**
# otherwise script will stop and ask for token
#
# export TWINE_PASSWORD="<provide token here>"

python -m twine upload dist/* --verbose
