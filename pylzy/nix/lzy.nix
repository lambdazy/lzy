{buildPythonPackage, botocore, requests, cloudpickle, pyyaml, boto3, stdlib-list, azure-storage-blob, grpclib, protobuf}:
buildPythonPackage rec {
    name = "lzy";
    version = (builtins.readFile ../version/version);
    src = ../.;

    propagatedBuildInputs = [
      botocore
      requests
      cloudpickle
      pyyaml
      boto3
      stdlib-list
      azure-storage-blob
      grpclib
      protobuf
    ];
    doCheck = false;
}
