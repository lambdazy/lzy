{ pkgs }:
let
  packageOverrides = self: super: rec {
    azure-storage-blob = super.azure-storage-blob.overridePythonAttrs(old: {
      doCheck = false;
    });
    boto3 = super.boto3.overridePythonAttrs(old: rec {
      version = "1.21.21";
      src = super.fetchPypi {
        pname = "boto3";
        inherit version;
        sha256 = "b6BiLzCM/R2nWJZvyYtS+9dLgGBtFFhsitgsemxPMtA=";
      };
    });

    botocore = super.botocore.overridePythonAttrs(old: rec {
      version = "1.24.21";
      src = super.fetchPypi {
        pname = "botocore";
        inherit version;
        sha256 = "fpds/QphYB50Yk749SRrQKAfLM5zoBHvKc+ApuNx0Po=";
      };

      propagatedBuildInputs = [
        super.urllib3
        super.python-dateutil
        jmespath
      ];
      doCheck = false;
    });

    coverage = super.coverage.overridePythonAttrs(old: rec {
      # propagatedBuildInputs = [
      #   trio
      # ];
      doCheck = false;
    });

    cloudpickle = super.cloudpickle.overridePythonAttrs(old: rec {
      pname = "cloudpickle";
      version = "2.1.0";
      src = super.fetchPypi {
        inherit pname version;
        sha256 = "bb233e876a58491d9590a676f93c7a5473a08f747d5ab9df7f9ce564b3e7938e";
      };
      propagatedBuildInputs = [
        super.pytest
        super.psutil
      ];
      doCheck = false;
    });

    importlib-metadata = super.importlib-metadata.overridePythonAttrs(old: rec {
      version = "4.8.1";
      src = super.fetchPypi {
        pname = "importlib_metadata";
        inherit version;
        sha256 = "f284b3e11256ad1e5d03ab86bb2ccd6f5339688ff17a4d797a0fe7df326f23b1";
      };
      doCheck = false;
    });

    jmespath = super.jmespath.overridePythonAttrs(old: rec {
      version = "0.7.1";
      src = super.fetchPypi {
        pname = "jmespath";
        inherit version;
        sha256 = "zVoS7j36RwKDoCCjXmnoOwcA1E/kEwFP01rVWExfX9E=";
      };
    });

    trio = super.trio.overridePythonAttrs(old: rec {
      doCheck = false;
    });

    catboost = super.catboost.overridePythonAttrs(old: rec {
      doCheck = false;
    });

    # doesn't work
    # protobuf = super.protobuf.overridePythonAttrs(old: rec {
    #   version = "4.21.5";
    #   src = super.fetchPypi {
    #     pname = "protobuf";
    #     inherit version;
    #     sha256 = "eb1106e87e095628e96884a877a51cdb90087106ee693925ec0a300468a9be3a";
    #   };

    #   propagatedBuildInputs = [
    #   ];
    #   doCheck = false;
    # });

    pure-protobuf = self.callPackage ./pure-protobuf.nix {
      inherit boto3;
      inherit botocore;
    };
    
    pyright = self.callPackage ./pyright.nix { };

    aioboto3 = self.callPackage ./aioboto3.nix { } ;

    # type stubs:
    botocore-stubs = self.callPackage ./botocore-stubs.nix { };

    boto3-stubs = self.callPackage ./boto3-stubs.nix {
      inherit botocore-stubs;
    };

    grpc-stubs = self.callPackage ./grpc-stubs.nix {
    };

    types-pyyaml = self.callPackage ./types-pyyaml.nix { };

    grpclib = self.callPackage ./grpclib.nix { };

    grpcio = self.callPackage ./grpcio.nix { };

    pycryptodome = self.callPackage ./pycryptodome.nix { };

    types-PyJWT = self.callPackage ./types-PyJWT.nix { };

    types-cryptography = self.callPackage ./types-cryptography.nix { };

    betterproto = self.callPackage ./betterproto.nix {
      inherit grpclib;
    };

    coverage-badge = self.callPackage ./coverage-badge.nix {
      inherit coverage;
    };
    types-six = self.callPackage ./types-six.nix {};
    types-chardet = self.callPackage ./types-chardet.nix {};
  };
in pkgs.python39.override {
  inherit packageOverrides;
  self = pkgs.python39;
}
