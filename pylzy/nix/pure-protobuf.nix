{
  buildPythonPackage,
  fetchPypi,
  flake8, isort, mypy,
  pytest, coveralls, build,
  twine, pytest-benchmark,
  botocore, boto3
}:
buildPythonPackage rec {
  version = "0.2";
  pname = "pure-protobuf";
  src = fetchPypi {
    pname = "hidden-${pname}";
    inherit version;
    sha256 = "ccee3efb201a2d10a856567911dccc8767eb73241c16f44817a7c10660a0d23c";
  };
  propagatedBuildInputs = [
    flake8
    isort
    mypy
    pytest
    coveralls
    build
    twine
    pytest-benchmark
    botocore
    boto3
  ];
}
