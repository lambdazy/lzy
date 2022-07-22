{ pkgs }:
let
  packageOverrides = self: super: rec {
    boto3 = super.boto3.overridePythonAttrs(old: rec {
      version = "1.20.24";
      src = super.fetchPypi {
        pname = "boto3";
        inherit version;
        sha256 = "c5cFso5rIynqO0gbqAHUOcKWqvF294UHKRR7qZu/ipo=";
      };
    });

    jmespath = super.jmespath.overridePythonAttrs(old: rec {
      version = "0.7.1";
      src = super.fetchPypi {
        pname = "jmespath";
        inherit version;
        sha256 = "zVoS7j36RwKDoCCjXmnoOwcA1E/kEwFP01rVWExfX9E=";
      };
    });

    botocore = super.botocore.overridePythonAttrs(old: rec {
      version = "1.23.24";
      src = super.fetchPypi {
        pname = "botocore";
        inherit version;
        sha256 = "QwBrT1LXu2VTGdPaD2Fc2+53YoU6zB68sdSfli5rSAY=";
      };

      propagatedBuildInputs = [
        super.urllib3
        super.python-dateutil
        jmespath
      ];
      doCheck = false;
    });

    pure-protobuf = self.callPackage ./pure-protobuf.nix {
      inherit boto3;
      inherit botocore;
    };
    
    cloudpickle = super.cloudpickle.overridePythonAttrs(old: rec {
      pname = "cloudpickle";
      version = "2.1.0";
      src = super.fetchPypi {
        inherit pname version;
        sha256 = "bb233e876a58491d9590a676f93c7a5473a08f747d5ab9df7f9ce564b3e7938e";
      };
      propagatedBuildInputs = [
        super.pytest super.psutil
      ];
      doCheck = false;
    });

    pyright = self.callPackage ./pyright.nix { };

    # type stubs:
    botocore-stubs = self.callPackage ./botocore-stubs.nix { };

    boto3-stubs = self.callPackage ./boto3-stubs.nix {
      inherit botocore-stubs;
    };

    grpc-stubs = self.callPackage ./grpc-stubs.nix {
    };

    types-pyyaml = self.callPackage ./types-pyyaml.nix { };

    grpclib = self.callPackage ./grpclib.nix { };

    betterproto = self.callPackage ./betterproto.nix {
      inherit grpclib;
    };
  };
in pkgs.python39.override {
  inherit packageOverrides;
  self = pkgs.python39;
}
