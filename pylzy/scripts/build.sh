#!/bin/bash

set -ex

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

echo "Building pylzy package"

rm -rvf "$src_dir/../build"
rm -rvf "$src_dir/../dist"
rm -rvf "$src_dir/../pylzy.egg-info"

# Generate grpc stubs
$src_dir/gen_proto.sh

# instead of
# python -m build
# which builds both wheel dist and source dist
#
# build by directly executing setup.py
# I couldn't find the better way to pass arguments to build
python setup.py clean --all sdist "$@" bdist_wheel "$@"  # pass --dev flag if needed
