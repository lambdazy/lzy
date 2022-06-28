#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

source ./util.sh

./gen_proto.sh

echo "Building pylzy package"
# TODO: pass jar path to script as parameter?
cp ../servant/target/servant-1.0-SNAPSHOT.jar lzy/lzy-servant.jar
[ $? -ne 0 ] && print_red "Failed to copy lzy-servant jar" && exit 1

# instead of
# python -m build
# which builds both wheel dist and source dist
# 
# build by directly executing setup.py
# I couldn't find the better way to pass arguments to build
python setup.py sdist "$@" bdist_wheel "$@"  # pass --dev flag if needed
