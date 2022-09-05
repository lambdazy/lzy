{
  buildPythonPackage,
  azure-storage-blob,
  botocore,
  boto3,
  cloudpickle,
  importlib-metadata,
  grpclib,
  pyyaml,
  protobuf,
  requests,
  stdlib-list,
  grpcio,
  pycryptodome
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
      importlib-metadata
      grpclib
      pyyaml
      protobuf
      requests
      stdlib-list
      grpcio
      pycryptodome
    ];
    doCheck = false;
}
