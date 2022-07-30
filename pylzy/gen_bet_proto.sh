#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

set -eux

source ./util.sh

print_green "Generating betterproto dataclasses stubs in $out"

cd "$proto_path"
# generation of some proto files is disabled for now
find . -iname "*.proto" -type f \
       ! -name "server.proto" \
       ! -name "kharon.proto" \
       -exec python -m grpc_tools.protoc -I . \
                    --python_betterproto_out="$OLDPWD" \
                    '{}' +
cd "$OLDPWD"

rm -v "__init__.py"
mv "ai/lzy/v1.py" "ai/lzy/v1/__init__.py"
mv "ai/lzy/v1/graph.py" "ai/lzy/v1/graph/__init__.py"

find "$proto_out" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
