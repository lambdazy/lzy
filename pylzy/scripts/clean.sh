#!/bin/bash

src_dir="$(dirname $0)"
cd "$src_dir/.."

echo "Clean up"

# tox envs
rm -rvf .tox

# linter and test outputs
rm -rvf .mypy_cache test_output

# package build artefacts
rm -rvf *.egg-info dist/ build/

# mvn build artefacts
rm -rvf target/

# generated proto files
[ ! -v 1 ] || rm -rvf "ai/" "google/"

# coverage report
rm -vf ./.coverage ./coverage.svg
rm -rvf htmlcov
