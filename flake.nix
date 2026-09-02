{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-parts,
      rust-overlay,
      crane,
      nixpkgs,
      git-hooks,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        git-hooks.flakeModule
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem =
        { config, system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          rustToolchain = pkgs.rust-bin.nightly.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
            targets = [ "wasm32-unknown-unknown" ];
          };

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        in
        {
          pre-commit.settings.hooks = {
            rustfmt = {
              enable = true;
              package = rustToolchain;
            };
            clippy = {
              enable = true;
              package = rustToolchain;
            };
            cargo-deny = {
              enable = true;
              name = "Cargo deny check";
              entry = "${pkgs.cargo-deny}/bin/cargo-deny check";
              files = "(Cargo\\.(toml|lock)|deny\\.toml)$";
              pass_filenames = false;
            };
          };

          devShells.default = pkgs.mkShell {
            name = "rust-nightly";

            shellHook = config.pre-commit.shellHook;

            packages = config.pre-commit.settings.enabledPackages ++ [
              rustToolchain
              pkgs.cargo-watch
              pkgs.cargo-deny
            ];
          };
        };
    };
}
