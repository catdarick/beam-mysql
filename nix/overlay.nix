self: super:
let
  inherit (self) fetchFromGitHub;

  bytestring-lexing-repo = fetchFromGitHub {
    owner = "juspay";
    repo = "bytestring-lexing";
    rev = "0a46db1139011736687cb50bbd3877d223bcb737";
    sha256 = "1jrwhlp8xs4m21xfr843278j3i7h4sxyjpq67l6lzc36pqan9zlz";
  };

  bytestring-lexing-path = bytestring-lexing-repo;

  mysql-haskell-repo = fetchFromGitHub {
    owner = "juspay";
    repo = "mysql-haskell";
    rev = "788022d65538db422b02ecc0be138b862d2e5cee";
    sha256 = "030qq1hgh15zkwa6j6x568d248iyfaw5idj2hh2mvb7j8xd1l4lv";
  };

  mysql-haskell-path = mysql-haskell-repo;

  beam-mysql-path = ../.;
in
super.eulerBuild.mkEulerHaskellOverlay self super
  (hself: hsuper:
    let 
    in {
      # Needed for floating point fix in mysql-haskell
      bytestring-lexing = self.eulerBuild.fastBuildExternal {
        drv = hself.callCabal2nix "bytestring-lexing" bytestring-lexing-path { };
      };

      # Uses bytestring-lexing
      binary-parsers = self.eulerBuild.fastBuildExternal {
        drv = hsuper.binary-parsers;
      };
      # Uses binary-parsers
      wire-streams = self.eulerBuild.fastBuildExternal {
        drv = hsuper.wire-streams;
      };

      mysql-haskell = self.eulerBuild.fastBuildExternal {
        drv = hself.callCabal2nix "mysql-haskell" mysql-haskell-path { };
      };

      beam-mysql = self.eulerBuild.fastBuildExternal {
        drv = hself.callCabal2nix "beam-mysql" beam-mysql-path { };
      };
  })
