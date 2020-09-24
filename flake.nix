{
  description = "beam-mysql";

  inputs.flake-utils.url = github:numtide/flake-utils;
  inputs.euler-build.url = flake:euler-build/wip;
  inputs.beam.url = github:juspay/beam/build-wip;

  outputs = { self, beam, euler-build, flake-utils }:
    { overlay = import ./overlay.nix; } //
    (flake-utils.lib.eachDefaultSystem (system:
      {
        defaultPackage = (import euler-build.nixpkgs {
          inherit system;
          overlays = [
            euler-build.overlay
            beam.overlay
            self.overlay
          ];
        }).eulerHaskellPackages.beam-mysql;
      }
    ));
      
}
