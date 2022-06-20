{pkgs}:
let
  packageOverrides = self: super: rec {
    pure-protobuf = super.botocore.overridePythonAttrs(old: rec {
      version = "0.2";
      src = super.fetchPypi {
        pname = "hidden-pure-protobuf";
        inherit version;
        sha256 = "ccee3efb201a2d10a856567911dccc8767eb73241c16f44817a7c10660a0d23c";
      };
      propagatedBuildInputs = [
        super.flake8
        super.isort
        super.mypy
        super.pytest
        super.coveralls
        super.build
        super.twine
        super.pytest-benchmark
        botocore
        boto3
      ];
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

    jmespath = super.jmespath.overridePythonAttrs(old: rec {
      version = "0.7.1";
      src = super.fetchPypi {
        pname = "jmespath";
        inherit version;
        sha256 = "zVoS7j36RwKDoCCjXmnoOwcA1E/kEwFP01rVWExfX9E=";
      };
    });

    boto3 = super.boto3.overridePythonAttrs(old: rec {
      version = "1.20.24";
      src = super.fetchPypi {
        pname = "boto3";
        inherit version;
        sha256 = "c5cFso5rIynqO0gbqAHUOcKWqvF294UHKRR7qZu/ipo=";
      };
    });
    
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
  };
in pkgs.python39.override {
  inherit packageOverrides;
  self = pkgs.python39;
}
