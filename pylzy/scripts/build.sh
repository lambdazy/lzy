#!/usr/bin/env nix-shell
#! nix-shell build.nix -A publish -i bash

set -ex

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

echo "Building pylzy package"
# TODO: pass jar path to script as parameter?
cp ../servant/target/servant-1.0-SNAPSHOT.jar lzy/lzy-servant.jar
[ $? -ne 0 ] && print_red "Failed to copy lzy-servant jar" && exit 1

# Generate grpc stubs
./gen_proto.sh

# instead of
# python -m build
# which builds both wheel dist and source dist
# 
# build by directly executing setup.py
# I couldn't find the better way to pass arguments to build
python setup.py sdist "$@" bdist_wheel "$@"  # pass --dev flag if needed
