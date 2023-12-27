{
  inputs = { nixpkgs.url = "nixpkgs/nixos-unstable"; };

  description = "Following the simple db tutorial";
  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

    in {
      devShells."${system}".default = pkgs.mkShell {
        buildInputs = with pkgs; [
          zig
          zls
          just
          haskellPackages.shelltestrunner
          (python3.withPackages (p: [ p.pytest ]))
        ];
      };
    };
}

