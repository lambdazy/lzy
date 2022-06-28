{pkgs, custom-python}:
pkgs.mkShell {
  buildInputs = [
    custom-python
  ];

  shellHook = ''
      # Tells pip to put packages into $PIP_PREFIX instead of the usual locations.
      # See https://pip.pypa.io/en/stable/user_guide/#environment-variables.
      #
      export PIP_PREFIX=$(pwd)/_build/pip_packages
      export PYTHONPATH=${custom-python}/${custom-python.sitePackages}
      export PATH="$PIP_PREFIX/bin:$PATH"
      unset SOURCE_DATE_EPOCH

      # call to list available executables
      alias exs="find . -maxdepth 1 -executable -type f"

      # proto paths
      export proto_out="lzy/proto"
      export proto_path="../model/src/main/proto/"
    '';
}
