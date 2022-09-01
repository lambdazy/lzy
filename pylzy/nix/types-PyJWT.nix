{
  buildPythonPackage,
  fetchPypi,
  types-cryptography
}:
buildPythonPackage rec {
  pname = "types-PyJWT";
  version = "1.7.1";

  src = fetchPypi {
    inherit pname version;
    sha256 = "99c1a0d94d370951f9c6e57b1c369be280b2cbfab72c0f9c0998707490f015c9";
  };

  propagatedBuildInputs = [
  types-cryptography
  ];
}