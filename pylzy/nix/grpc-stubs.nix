{
  buildPythonPackage,
  fetchPypi,
}:
buildPythonPackage rec {
  pname = "grpc-stubs";
  version = "1.24.10";

  src = fetchPypi {
    inherit pname version;
    sha256 = "92460dbabea0e77e34241afe7594b86f5cef2f9ae6ca0230f1ae2430427c20f9";
  };

  propagatedBuildInputs = [

  ];
}
