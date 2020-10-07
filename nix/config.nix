{ nixpkgs }:
{
  flakeName = "beam-mysql";
  defaultPackageName = "beam-mysql";
  exportPackages = [
    "beam-mysql"
  ];

  shellTools =
    with nixpkgs; [
      cabal-fmt
    ];
  # shellAttrs = {
  #   withHoogle = false;
  # };
}