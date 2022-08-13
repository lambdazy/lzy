{ pkgs }:
let
  python = pkgs.callPackage ./python.nix {};
  lzy = ps: ps.callPackage ./lzy.nix {};
  python_dev_deps = ps: with ps; [
    boto3
    cloudpickle
    pyyaml
    importlib-metadata
    wheel
    azure-storage-blob
    stdlib-list
    aioboto3

    pip

    # it's here just to get protoc
    protobuf
    grpcio-status
    grpcio-tools
    grpclib
    betterproto
    mypy-protobuf
  ];
  python_lzy_deps = ps: (python_dev_deps ps) ++ (with ps; [
    (lzy ps)
  ]);
  python_tests_deps = ps: (python_lzy_deps ps) ++ (with ps; [
    # too long, install through pip
    # catboost
    
    # needed for tests/serialization/test_serializer.py
    pure-protobuf

    coverage
    coverage-badge
  ]);
  python-dev = python.withPackages(python_dev_deps);
  python-tests = python.withPackages(python_tests_deps);
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
    grpclib
    types-setuptools
    types-requests
    types-protobuf
    types-six
    types-chardet

    # custom
    boto3-stubs
    types-pyyaml

    # some libs to have types
    cloudpickle
  ]);

  mkEnv = custom-python: pkgs.callPackage ./mk-python-env.nix {
    inherit custom-python;
  };
in {
  shell = mkEnv python-dev;
  shell-tests = mkEnv python-tests;
  shell-publish = mkEnv python-publish;
  shell-lint = mkEnv python-lint;
}
