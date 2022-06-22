{pkgs}:
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
      version = "2.0.0";
      src = super.fetchPypi {
        inherit pname version;
        sha256 = "XNAvO0F6eDuoSk7D4pD/eSkAn+UfZAVCPPzPrdQ7pKQ=";
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

    types-pyyaml = self.callPackage ./types-pyyaml.nix { };

    grpclib = self.callPackage ./grpclib.nix { };
  };
in pkgs.python39.override {
  inherit packageOverrides;
  self = pkgs.python39;
}
