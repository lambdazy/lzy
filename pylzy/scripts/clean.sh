#!/bin/bash

echo "Clean up"

# package build artefacts
rm -rvf *.egg-info dist/ build/ lzy/lzy-servant-*.jar

# linter and test outputs
rm -rvf .mypy_cache test_output

# pip installed libs in nix env
rm -rvf _build/

# mvn build artefacts
rm -rvf target/

# generated proto files
[ ! -v 1 ] || rm -rvf "ai/"

# coverage report
rm -vf ./.coverage ./coverage.svg
rm -rvf htmlcov


