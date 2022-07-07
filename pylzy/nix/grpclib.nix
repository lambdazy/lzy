{
  buildPythonPackage,
  fetchPypi,
  protobuf,
  h2,
  multidict,
  typing-extensions,
}:
buildPythonPackage rec {
  version = "0.4.2";
  pname = "grpclib";
  src = fetchPypi {
    inherit pname version;
    sha256 = "ead080cb7d56d6a5e835aaf5255d1ef1dce475a7722566ea225f0188fce33b68";
  };
  propagatedBuildInputs = [
    h2
    multidict
    typing-extensions
    protobuf
  ];
}
