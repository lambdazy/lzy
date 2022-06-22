{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/360d84a26f0cfca4941d92a89e2b359d425a6d3d.tar.gz") {} }:
let
  lzy = ps: ps.callPackage ./nix/lzy.nix {};
  python = pkgs.callPackage ./nix/python.nix {};
  python-default = python.withPackages(ps: with ps; [
    boto3
    cloudpickle
    pyyaml
    importlib-metadata
    wheel
    azure-storage-blob
    requests
    stdlib-list
    pure-protobuf
  ]);
  python-lzy = python.withPackages(ps: with ps; [
    (lzy ps)
  ]);
  python-publish = python.withPackages(ps: with ps; [
    wheel
    build
    twine
  ]);
  python-lint = python.withPackages(ps: with ps; [
    mypy
    pyright

    types-setuptools
    types-requests

    # custom
    boto3-stubs
    types-pyyaml
  ]);
  mkEnv = custom-python: pkgs.callPackage ./nix/mk-python-env.nix {
    inherit custom-python;
  };
in {
  shell = mkEnv python-default;
  shell-lzy = mkEnv python-lzy;
  shell-publish = mkEnv python-publish;
  shell-lint = mkEnv python-lint;
}
