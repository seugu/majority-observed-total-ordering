{
  pkgs ? import <nixpkgs> { },
}:

let
  inherit (pkgs) lib stdenv;

in pkgs.mkShell {
  inputsFrom = [
    pkgs.androidShell
  ];

  buildInputs = with pkgs; [
    nim-2_2
    nimble
    which
    git
    cmake
  ] ++ lib.optionals stdenv.isDarwin [
    pkgs.libiconv
  ];

  # Avoid compiling Nim itself.
  # Setting nim cache to proper tmp location avoids cache collision in CI
  shellHook = ''
    export USE_SYSTEM_NIM=1
    export XDG_CACHE_HOME="$TMPDIR"
  '';
}
