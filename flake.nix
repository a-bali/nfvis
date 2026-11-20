{
  description = "nfvis - homelab NetFlow collector and dashboard development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          name = "nfvis-dev";

          packages = with pkgs; [
            go
            nodejs_24
            gnumake
            git
            curl
            jq
            sqlite
          ];

          shellHook = ''
            case $- in
              *i*)
                echo "nfvis dev shell"
                echo "  go:    $(go version)"
                echo "  node:  $(node --version)"
                echo "  npm:   $(npm --version)"
                echo "Run 'make help' for available targets."
                ;;
            esac
          '';
        };
      });
}
