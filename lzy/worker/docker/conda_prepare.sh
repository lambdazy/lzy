#!/bin/bash

install_lzy_in_every_env() {
  path="$1"
  cd "$path"
  for env in $(conda env list | cut -d" " -f1 | tail -n+4);
  do
    conda activate "$env" && pip install -r requirements.txt && ./scripts/build.sh && pip install .
  done
  cd -
}

create_env() {
  name="$1"
  version="$2"
  conda create --name "$name" "python=$version" pip && \
  conda activate "$name" && \
  python -m ensurepip && \
  pip install -r requirements.txt && \
  return 0
}

create_envs() {
  create_env "default" "3.9.7" \
      && create_env "py37" "3.7.11" \
      && create_env "py38" "3.8.12" \
      && create_env "py39" "3.9.7" \
  #    && create_env "py310" "3.10.0"
}

command="$1";
eval "$(conda shell.bash hook)";

case "$command" in
  "pylzy_install") lzy_python="$2" && install_lzy_in_every_env "$lzy_python";;
  "create_env") create_env "$2" "$3";;
  "init") create_envs;;
  *) echo "Invalid prepare command" && exit 1;;
esac
