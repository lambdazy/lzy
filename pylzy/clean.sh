#!/usr/bin/env sh

echo "Clean up"
# clean package build artefacts
rm -rvf *.egg-info dist/ build/ lzy/lzy-servant-*.jar

# clean linter and test outputs
rm -rvf .mypy_cache test_output

# clean pip installed libs in nix env
rm -rvf _build/

# clean mvn build artefacts
rm -rvf target/ 

# remove generated proto files
rm -rvf "$proto_out"
