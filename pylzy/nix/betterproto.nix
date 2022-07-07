{
  buildPythonPackage,
  fetchPypi,
  grpclib,
  python-dateutil,
  black,
  jinja2,
  stringcase,
}:
buildPythonPackage rec {
  pname = "betterproto";
  version = "1.2.5";

  src = fetchPypi {
    inherit pname version;
    sha256 = "74a3ab34646054f674d236d1229ba8182dc2eae86feb249b8590ef496ce9803d";
  };
  propagatedBuildInputs = [
    python-dateutil
    grpclib
    stringcase

    jinja2
    black
  ];
}
