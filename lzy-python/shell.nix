{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/360d84a26f0cfca4941d92a89e2b359d425a6d3d.tar.gz") {} }:
let
  build = pkgs.callPackage ./build.nix {};
in
  build.shell
