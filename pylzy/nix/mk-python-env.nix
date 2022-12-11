{pkgs, custom-python}:
pkgs.mkShell {
  buildInputs = [
    custom-python
  ];

  shellHook = ''
      # Tells pip to put packages into $PIP_PREFIX instead of the usual locations.
      # See https://pip.pypa.io/en/stable/user_guide/#environment-variables.
      #

      export PIP_PREFIX="$(pwd)/_build/pip_packages"
      export PYTHONPATH="${custom-python}/${custom-python.sitePackages}:$PYTHONPATH"
      export PYTHONPATH="$PIP_PREFIX/${custom-python.sitePackages}:$PYTHONPATH"

      export PATH="$PIP_PREFIX/bin:$PATH"
      unset SOURCE_DATE_EPOCH

      # call to list available executables
      alias exs="find . -maxdepth 1 -executable -type f"

      # proto paths
      export proto_out="$(pwd)/ai/lzy/v1"
      export proto_validation_path="$(pwd)/../util/util-grpc/src/main/proto/"
      export proto_model_path="$(pwd)/../model/src/main/proto/"
      export proto_workflow_path="$(pwd)/../lzy-api/src/main/proto/"
    '';
}
