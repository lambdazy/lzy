{
  buildPythonPackage,
  azure-storage-blob,
  botocore,
  boto3,
  cloudpickle,
  jsonpickle,
  importlib-metadata,
  grpclib,
  pyyaml,
  protobuf,
  requests,
  stdlib-list,
  grpcio,
  pycryptodome,
  aioboto3
}:
buildPythonPackage rec {
    name = "lzy";
    version = (builtins.readFile ../version/version);
    src = ../.;

    propagatedBuildInputs = [
      azure-storage-blob
      botocore
      boto3
      cloudpickle
      jsonpickle
      importlib-metadata
      grpclib
      pyyaml
      protobuf
      requests
      stdlib-list
      grpcio
      pycryptodome
      aioboto3
    ];
    doCheck = false;
}
