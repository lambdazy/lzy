{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  pname = "types-PyYAML";
  version = "6.0.8";
  src = fetchPypi {
    inherit pname version;
    sha256 = "d9495d377bb4f9c5387ac278776403eb3b4bb376851025d913eea4c22b4c6438";
  };
  propagatedBuildInputs = [
  ];
}
