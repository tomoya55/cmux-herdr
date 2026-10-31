# Source this file in zsh to monitor interactive Herdr clients inside cmux.
typeset -g CMUX_HERDR_SCRIPT="${${(%):-%x}:A:h}/cmux_herdr.py"

herdr() {
  if [[ -z ${CMUX_WORKSPACE_ID:-} || -z ${CMUX_SURFACE_ID:-} ]]; then
    command herdr "$@"
    return $?
  fi

  local cmux_herdr_binary
  cmux_herdr_binary=$(whence -p herdr)
  HERDR_BIN_PATH="$cmux_herdr_binary" python3 "$CMUX_HERDR_SCRIPT" supervisor "$@" </dev/null >/dev/null 2>&1 &!
  command herdr "$@"
  local cmux_herdr_exit=$?
  return "$cmux_herdr_exit"
}
