{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  pname = "catboost";
  version = "1.0.6";

  src = fetchPypi {
    inherit pname version;
    sha256 = "6eb0541591dd81fa7541666ed011f02613cc6715c38c8467a81914eae5c48420";
  };
  propagatedBuildInputs = [
  ];
}
