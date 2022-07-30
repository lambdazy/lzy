{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  version = "5.0.3";
  pname = "types-chardet";
  src = fetchPypi {
    inherit pname version;
    sha256 = "ff45a67b6d126deeabf43ffa72e17fa5914fb37f74cf3bd64da4419bc8248ca1";
  };
  propagatedBuildInputs = [];
}
