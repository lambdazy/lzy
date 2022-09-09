{
  buildPythonPackage,
  jinja2,
  requests,
  flask-cors,
  boto3,
  responses,
  fetchPypi,
  pytz,
  xmltodict,
  flask,
  werkzeug
}:
buildPythonPackage rec {
  pname = "moto";
  version = "4.0.2";
  src = fetchPypi {
    inherit pname version;
    sha256 = "231836b76ceb1786f4e91dae77e9d34e037380764edd9fd55dffa42781c8e4e7";
  };
  propagatedBuildInputs = [
  werkzeug
  jinja2
  boto3
  pytz
  flask-cors
  flask
  responses
  requests
  xmltodict
  ];
  doCheck = false;
}