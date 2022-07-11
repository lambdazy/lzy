{ pkgs }:
let
  python = pkgs.callPackage ./nix/python.nix {};

  lzy = ps: ps.callPackage ./nix/lzy.nix {};

  python-dev = python.withPackages(ps: with ps; [
    pip
    boto3
    cloudpickle
    pyyaml
    importlib-metadata
    wheel
    azure-storage-blob
    requests
    stdlib-list
    pure-protobuf

    pip

    # it's here just to get protoc
    protobuf
    grpcio-tools
    grpclib
    betterproto
    mypy-protobuf
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
    # formatter
    black

    # import sort
    isort

    # typing
    mypy
    pyright

    # type stubs:
    types-setuptools
    types-requests
    types-protobuf

    # custom
    boto3-stubs
    types-pyyaml

    # some libs to have types
    cloudpickle
    pure-protobuf
  ]);

  mkEnv = custom-python: pkgs.callPackage ./nix/mk-python-env.nix {
    inherit custom-python;
  };
in {
  shell = mkEnv python-dev;
  shell-lzy = mkEnv python-lzy;
  shell-publish = mkEnv python-publish;
  shell-lint = mkEnv python-lint;
}
