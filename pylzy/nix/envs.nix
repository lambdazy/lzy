{ pkgs }:
let
  python = pkgs.callPackage ./python.nix {};

  lzy = ps: ps.callPackage ./lzy.nix {};

  mkEnv = custom-python: pkgs.callPackage ./mk-python-env.nix {
    inherit custom-python;
  };

  deps = rec {
    dev = ps: with ps; [
      boto3
      cloudpickle
      pyyaml
      importlib-metadata
      wheel
      azure-storage-blob
      stdlib-list
      aioboto3
      pycryptodome
      pure-protobuf

      pip

      # it's here just to get protoc
      protobuf
      grpcio-status
      grpcio-tools
      grpcio
      betterproto
      mypy-protobuf
    ];

    tests = ps: with ps; [
      (lzy ps)
      # too long, install through pip
      # catboost

      # needed for tests/serialization/test_serializer.py
      pure-protobuf
      moto
      requests

      coverage
      coverage-badge
    ];

    publish = ps: with ps; [
      wheel
      build
      twine
    ];

    lint = ps: with ps; [
      # formatter
      black

      # import sort
      isort

      # typing
      mypy
      pyright

      # type stubs:
      grpcio
      grpc-stubs
      types-PyJWT
      types-cryptography
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
    ];
  };

  shells = builtins.mapAttrs (name: value: mkEnv (python.withPackages(value))) deps;
in shells
