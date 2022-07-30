{
  buildPythonPackage,
  fetchPypi,
  typing-extensions,
  botocore-stubs
}:
buildPythonPackage rec {
  pname = "boto3-stubs";
  version = "1.24.14";
  src = fetchPypi {
    inherit pname version;
    sha256 = "e4d75b25c0e8c4af2c4b60755f890c2d5b948cdee7fc8d22e4935aa4a5c5ec5f";
  };
  propagatedBuildInputs = [
    typing-extensions
    botocore-stubs
  ];
}
