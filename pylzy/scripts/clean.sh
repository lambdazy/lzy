#!/bin/bash

echo "Clean up"

# linter and test outputs
rm -rvf .mypy_cache test_output

# package build artefacts
rm -rvf *.egg-info dist/ build/

# mvn build artefacts
rm -rvf target/

# generated proto files
[ ! -v 1 ] || rm -rvf "ai/"

# coverage report
rm -vf ./.coverage ./coverage.svg
rm -rvf htmlcov

# readme & license
rm readme.md LICENSE


