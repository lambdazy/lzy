{
  buildPythonPackage,
  fetchPypi,
  protobuf,
}:
buildPythonPackage rec {
  pname = "grpcio";
  version = "1.47.0";
  src = fetchPypi {
    inherit pname version;
    sha256 = "5dbba95fab9b35957b4977b8904fc1fa56b302f9051eff4d7716ebb0c087f801";
  };
  propagatedBuildInputs = [
  protobuf
  ];
  doCheck = false;
}