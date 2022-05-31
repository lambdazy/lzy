# save this as shell.nix
{ pkgs ? import <nixpkgs> {}}:
let
  build = pkgs.callPackage ./build.nix {};
in
  build.shell
