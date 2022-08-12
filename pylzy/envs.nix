{ pkgs }:
let
  python = pkgs.callPackage ./nix/python.nix {};
  lzy = ps: ps.callPackage ./nix/lzy.nix {};
  python_dev_deps = ps: with ps; [
    boto3
    cloudpickle
    pyyaml
    importlib-metadata
    wheel
    azure-storage-blob
    requests
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

    # for tests
    pure-protobuf
    catboost
  ];
  python_lzy_deps = ps: (python_dev_deps ps) ++ (with ps; [
    (lzy ps)
    coverage
    coverage-badge
  ]);
  python-dev = python.withPackages(python_dev_deps);
  python-lzy = python.withPackages(python_lzy_deps);
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
