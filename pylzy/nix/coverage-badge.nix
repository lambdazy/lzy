{
  buildPythonPackage,
  fetchPypi,
  coverage
}:
buildPythonPackage rec {
  pname = "coverage-badge";
  version = "1.1.0";
  src = fetchPypi {
    inherit pname version;
    sha256 = "c824a106503e981c02821e7d32f008fb3984b2338aa8c3800ec9357e33345b78";
  };
  propagatedBuildInputs = [
    coverage
  ];
}
