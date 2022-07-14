#!/usr/bin/env sh

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
rm -rvf "$proto_out/priv" "$proto_out/v1"
