{ }:
let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/archive/360d84a26f0cfca4941d92a89e2b359d425a6d3d.tar.gz";
  pkgs = import (nixpkgs) {};
in pkgs.callPackage ../nix/envs.nix { inherit pkgs; }
