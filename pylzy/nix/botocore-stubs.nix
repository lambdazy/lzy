{
  buildPythonPackage,
  fetchPypi,
  typing-extensions
}:
buildPythonPackage rec {
  pname = "botocore-stubs";
  version = "1.27.14";
  src = fetchPypi {
    inherit pname version;
    sha256 = "18006cf1fa064e9914f22ddede07f5d88baee4a88bbc8cc0f69491e3c73dc076";
  };
  propagatedBuildInputs = [
    typing-extensions
  ];
}

