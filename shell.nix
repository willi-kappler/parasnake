{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    # nativeBuildInputs is usually what you want -- tools you need to run
    nativeBuildInputs = with pkgs.buildPackages; [
      (python312.withPackages(ps: with ps; [
        cryptography
        flake8
        hatchling
        ipython
        mypy
        ]))
      pyright
      ruff
    ];
}

