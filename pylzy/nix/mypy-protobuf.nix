{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  pname = "mypy-protobuf";
  version = "3.2.0";

  src = fetchPypi {
    inherit pname version;
    sha256 = "730aa15337c38f0446fbe08f6c6c2370ee01d395125369d4b70e08b1e2ee30ee";
  };
  propagatedBuildInputs = [
  ];
}
