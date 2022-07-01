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
<<<<<<< HEAD
      export proto_out="$(pwd)/ai/lzy/v1"
=======
      export proto_out="$(pwd)/lzy/proto"
>>>>>>> 31dea042 (Stable gen_proto.sh, fix import hell and show .pyi files too)
      export proto_path="$(pwd)/../model/src/main/proto/"
    '';
}
