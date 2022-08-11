{
  buildPythonPackage,
  fetchPypi,
  aiobotocore,
  boto3,
}:
buildPythonPackage rec {
  pname = "aioboto3";
  version = "9.6.0";
  src = fetchPypi {
    inherit pname version;
    sha256 = "abac5dcfa871627b7040e6586a69e3359e2dfc4e15dc66135969f2d26fbdcb3b";
  };
  propagatedBuildInputs = [
    aiobotocore
    boto3
  ];
  doCheck = false;
}

