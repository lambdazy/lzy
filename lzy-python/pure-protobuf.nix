{buildPythonPackage, fetchgit, flake8, isort, mypy, pytest, coveralls, build, twine, pytest-benchmark}:
buildPythonPackage rec {
    pname = "pure-protobuf";
    name = "pure-protobuf";
    version = "2.1.0";
    src = fetchgit {
      url = "https://github.com/eigenein/protobuf.git";
      rev = "2699b92fafc212ab3e7278aaa3931e9f4d326308";
      sha256 = "jLrDzHIEuOE90+87YnN0PLQ8gSb5QEuMOsrVaDBANMA=";
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
    ];
}
