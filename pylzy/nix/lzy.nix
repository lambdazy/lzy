{buildPythonPackage, botocore, requests, cloudpickle, pyyaml, boto3, pure-protobuf, stdlib-list, azure-storage-blob}:
buildPythonPackage rec {
    name = "lzy";
    version = "2.1.0";
    src = ./.;

    propagatedBuildInputs = [
      botocore
      requests
      cloudpickle
      pyyaml
      boto3
      pure-protobuf
      stdlib-list
      azure-storage-blob
    ];
    doCheck = false;
}
