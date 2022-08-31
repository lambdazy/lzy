{
  buildPythonPackage,
  fetchPypi
}:
buildPythonPackage rec {
  pname = "types-cryptography";
  version = "3.3.23";

  src = fetchPypi {
    inherit pname version;
    sha256 = "b85c45fd4d3d92e8b18e9a5ee2da84517e8fff658e3ef5755c885b1c2a27c1fe";
  };

  propagatedBuildInputs = [
  ];
}