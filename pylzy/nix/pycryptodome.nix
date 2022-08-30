{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  pname = "pycryptodome";
  version = "3.15.0";
  src = fetchPypi {
    inherit pname version;
    sha256 = "9135dddad504592bcc18b0d2d95ce86c3a5ea87ec6447ef25cfedea12d6018b8";
  };
  propagatedBuildInputs = [
  ];
  doCheck = false;
}