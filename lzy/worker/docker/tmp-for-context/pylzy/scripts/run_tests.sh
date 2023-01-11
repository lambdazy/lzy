#!/bin/bash

./scripts/build.sh
pip install tox -U
tox
