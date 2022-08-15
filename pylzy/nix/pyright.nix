{
  buildPythonPackage,
  fetchPypi,
  nodeenv
}:
buildPythonPackage rec {
  pname = "pyright";
  version = "1.1.255";
  src = fetchPypi {
    inherit pname version;
    sha256 = "ccfe8796a0264fcaa2148bc4b9c348e99ad4e77482307371bd0682bd20c6617c";
  };
  propagatedBuildInputs = [
    nodeenv
  ];
  doCheck = false;
}

