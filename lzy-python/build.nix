{ pkgs ? import <nixpkgs> {}}:
let
  pure-protobuf = ps: ps.callPackage ./pure-protobuf.nix {};
  m-botocore = ps: ps.callPackage ./botocore.nix {};

  lzy = ps: ps.callPackage ./lzy.nix {
    botocore = (m-botocore ps);
    pure-protobuf = (pure-protobuf ps);
  };

  python = let
    packageOverrides = self: super: {
      botocore = super.botocore.overridePythonAttrs(old: rec {
        version = "1.26.10";
        src = super.fetchPypi {
          pname = "botocore";
          inherit version;
          sha256 = "5df2cf7ebe34377470172bd0bbc582cf98c5cbd02da0909a14e9e2885ab3ae9c";
        };

        propagatedBuildInputs = [ super.jmespath super.urllib3 super.python-dateutil ];
        doCheck = false;
      });
      boto3 = super.boto3.overridePythonAttrs(old: rec {
        version = "1.23.10";
        src = super.fetchPypi {
          pname = "boto3";
          inherit version;
          sha256 = "2a4395e3241c20eef441d7443a5e6eaa0ee3f7114653fb9d9cef41587526f7bd";
        };
      });
      cloudpickle = super.cloudpickle.overridePythonAttrs(old: rec {
        pname = "cloudpickle";
        version = "2.1.0";
        src = super.fetchPypi {
          inherit pname version;
          sha256 = "bb233e876a58491d9590a676f93c7a5473a08f747d5ab9df7f9ce564b3e7938e";
        };

        propagatedBuildInputs = [ super.pytest super.psutil ];
        doCheck = false;
      });
    };
    in pkgs.python39.override {inherit packageOverrides; self = python;};

  customPython = python.withPackages(ps: with ps; [
      pip
      pyyaml
      importlib-metadata
      wheel
      requests
      azure-storage-blob
      stdlib-list
      cryptography
      guppy3
      (pure-protobuf ps)
  ]);

  python-lzy = customPython.withPackages(ps: with ps; [
      (lzy ps)
  ]);
in {
  shell = customPython.env;
  shell-lzy = python-lzy.env;

  test = pkgs.stdenv.mkDerivation rec {
    name = "tests";
    buildInputs = [ python-lzy ];
    src = ./.;
    buildPhase = ''
      python setup.py install --prefix $out
    '';
    installPhase = ''
      python -m unittest discover -s ${src}/tests > $out/test_output
      cat $out/test_output
    '';
    system = builtins.currentSystem;
  };
}
