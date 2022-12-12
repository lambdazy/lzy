#!/bin/bash

echo "Publishing package"

export TWINE_USERNAME="__token__"
# **token has to be provided by person or bot who runs the script**
# otherwise script will stop and ask for token
#
# export TWINE_PASSWORD="<provide token here>"

pip install twine -U
python -m twine upload dist/* --verbose
