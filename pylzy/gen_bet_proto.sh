#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

set -eux

source ./util.sh


out="$proto_out/bet"
print_green "Generating betterproto dataclasses stubs in $out"


[ ! -d "$out" ] && mkdir -p "$out"

cd "$proto_path"
# generation of some proto files is disabled for now
find . -iname "*.proto" -type f \
       ! -name "lzy-server.proto" \
       ! -name "lzy-kharon.proto" \
       -exec python -m grpc_tools.protoc -I . \
                    --python_betterproto_out="$out" \
                    '{}' +

cd "$out"
mv lzy/proto/* ./
find . -name "__init__.py" -exec rm -v {} +
rmdir -p "lzy/proto"

find "$proto_out/bet" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
