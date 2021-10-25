#!/usr/bin/env sh

install_interpreter() {
  version="$1"
  echo "Installing Python-$version"
  wget "https://www.python.org/ftp/python/$version/Python-$version.tgz"
  tar xzvf "Python-$version.tgz"
  cd "Python-$version" || return 1
  ./configure
  make
  sudo make altinstall
  return 0
}


while [ "$#" -ne 0 ] && install_interpreter "$1"; do
  shift
done
