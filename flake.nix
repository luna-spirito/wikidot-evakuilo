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
      self,
      flake-parts,
      rust-overlay,
      crane,
      nixpkgs,
      git-hooks,
      ...
    }:
    let
      # The build, as a pure function of its toolchain. `perSystem` below
      # instantiates it with this flake's own default (`nightly.latest`);
      # dentrado re-instantiates it with its pinned nightly, so one rustc
      # builds every package the deployment ships.
      mkEvakuilo =
        { pkgs, rustToolchain }:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          src = craneLib.cleanCargoSource ./.;

          commonArgs = {
            inherit src;
            strictDeps = true;
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        {
          evakuilo = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
            meta = {
              description = "Wikidot evacuation archiver: SQLite-first scrape state, per-page zstd publication tree";
              mainProgram = "evakuilo";
              license = pkgs.lib.licenses.agpl3Plus;
            };
          });

          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };
    in
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

      flake.nixosModules = {
        evakuilo = import ./nix/nixos.nix self;
        default = self.nixosModules.evakuilo;
      };

      flake.lib = { inherit mkEvakuilo; };

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

          built = mkEvakuilo { inherit pkgs rustToolchain; };
        in
        {
          packages = {
            inherit (built) evakuilo;
            default = built.evakuilo;
          };

          # Formatting is enforced by the pre-commit rustfmt hook instead:
          # nightly rustfmt style drift would randomly break `nix flake check`.
          checks = built;

          # Keep the hooks devshell/commit-only: the cargo-deny advisories
          # check fetches the RustSec DB and hangs in the offline Nix
          # sandbox; compile/lint/test coverage in `nix flake check` comes
          # from the crane checks above.
          pre-commit.check.enable = false;

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
