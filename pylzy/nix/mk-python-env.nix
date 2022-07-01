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
      export PYTHONPATH="$PYTHONPATH:${custom-python}/${custom-python.sitePackages}"
      export PYTHONPATH="$PYTHONPATH:$PIP_PREFIX/${custom-python.sitePackages}"
      export PATH="$PIP_PREFIX/bin:$PATH"
      unset SOURCE_DATE_EPOCH

      # call to list available executables
      alias exs="find . -maxdepth 1 -executable -type f"

      # proto paths
      export proto_out="$(pwd)/lzy/proto"
      export proto_path="$(pwd)/../model/src/main/proto/"
    '';
}
