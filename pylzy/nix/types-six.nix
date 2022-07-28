{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  version = "1.16.18";
  pname = "types-six";
  src = fetchPypi {
    inherit pname version;
    sha256 = "26f481fabb65321ba428bdfb82c97fc638e00be6b20efa83915b007cf3893e28";
  };
  propagatedBuildInputs = [];
}
