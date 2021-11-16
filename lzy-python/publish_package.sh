#!/bin/bash
# ! this script should be ran from lzy-servant directory


python -m pip install --upgrade build twine
# TODO: pass jar path to script as parameter?
cp ../lzy-servant/target/lzy-servant-1.0-SNAPSHOT-jar-with-dependencies.jar lzy/lzy-servant.jar
python -m build # builds both wheel dist and source dist

# currently publish to testpypi repo later should be changed to real pypi
# token has to be provided by person who runs the script
python -m twine upload dist/* --verbose


