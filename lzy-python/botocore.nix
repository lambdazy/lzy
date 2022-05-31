{fetchPypi, buildPythonPackage, jmespath, urllib3, python-dateutil}:
buildPythonPackage rec {
    pname = "botocore";
    version = "1.26.10";
    src = fetchPypi {
      inherit pname version;
      sha256 = "5df2cf7ebe34377470172bd0bbc582cf98c5cbd02da0909a14e9e2885ab3ae9c";
    };
    propagatedBuildInputs = [ jmespath urllib3 python-dateutil ];
    doCheck = false;
}
