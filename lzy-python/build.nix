{ pkgs ? import <nixpkgs> {}}:
let
  lzy = ps: ps.callPackage ./lzy.nix {};
  python = pkgs.callPackage ./python.nix {};
  python-lzy = python.withPackages(ps: with ps; [
      (lzy ps)
  ]);
in {
  shell = pkgs.callPackage ./mk-python-env.nix { custom-python = python; };
  shell-lzy = pkgs.callPackage ./mk-python-env.nix { custom-python = python-lzy; };
}
