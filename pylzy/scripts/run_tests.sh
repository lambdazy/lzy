#!/bin/bash

./scripts/build.sh

if [[ $(uname -p) == 'arm' ]]; then
  pip uninstall -y grpcio
  conda install -y grpcio
fi

pip install tox -U
tox
