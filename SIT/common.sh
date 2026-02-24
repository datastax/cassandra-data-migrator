# Container runtime configuration
export CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-auto}"
export CONTAINER_RUNTIME_FORCE="${CONTAINER_RUNTIME_FORCE:-false}"
export CONTAINER_RUNTIME_CMD=""  # Set by _initContainerRuntime()

# Container names (backward compatible)
export CONTAINER_CASS="${CONTAINER_CASS:-cdm-sit-cass}"
export CONTAINER_CDM="${CONTAINER_CDM:-cdm-sit-cdm}"
export DOCKER_CASS="${CONTAINER_CASS}"  # Backward compatibility alias
export DOCKER_CDM="${CONTAINER_CDM}"    # Backward compatibility alias

export CASS_USERNAME=cassandra
export CASS_PASSWORD=cassandra
export KEYSPACES="origin target"
export CDM_DIRECTORY=/local
export CDM_JARFILE=cassandra-data-migrator.jar

_info() {
  echo "INFO $*"
}

_warn() {
  echo "WARN $*"
}

_error() {
  echo "ERROR $*"
}

_fatal() {
  echo "FATAL $*"
  exit 2
}

# Container runtime detection and initialization
_detectContainerRuntime() {
  local runtime="${CONTAINER_RUNTIME:-auto}"
  local force="${CONTAINER_RUNTIME_FORCE:-false}"
  
  if [[ "$runtime" == "auto" ]]; then
    # Try Podman first
    if command -v podman >/dev/null 2>&1; then
      echo "podman"
      return 0
    fi
    # Fallback to Docker
    if command -v docker >/dev/null 2>&1; then
      echo "docker"
      return 0
    fi
    return 1
  else
    # Explicit runtime specified
    if command -v "$runtime" >/dev/null 2>&1; then
      echo "$runtime"
      return 0
    fi
    
    # Try fallback if not forced
    if [[ "$force" != "true" ]]; then
      local fallback
      if [[ "$runtime" == "podman" ]]; then
        fallback="docker"
      else
        fallback="podman"
      fi
      
      if command -v "$fallback" >/dev/null 2>&1; then
        _warn "Requested runtime '$runtime' not available, using '$fallback'"
        echo "$fallback"
        return 0
      fi
    fi
    
    return 1
  fi
}

_validateContainerRuntime() {
  local runtime="$1"
  
  _info "Validating container runtime: $runtime"
  
  # Test basic commands
  $runtime --version >/dev/null 2>&1 || return 1
  
  # Test network support
  $runtime network ls >/dev/null 2>&1 || return 1
  
  _info "Runtime validation successful"
  return 0
}

_initContainerRuntime() {
  CONTAINER_RUNTIME_CMD=$(_detectContainerRuntime)
  local rtn=$?
  
  if [[ $rtn -ne 0 || -z "$CONTAINER_RUNTIME_CMD" ]]; then
    _fatal "No container runtime available. Please install Podman or Docker."
  fi
  
  if ! _validateContainerRuntime "$CONTAINER_RUNTIME_CMD"; then
    _fatal "Container runtime '$CONTAINER_RUNTIME_CMD' validation failed"
  fi
  
  _info "Using container runtime: $CONTAINER_RUNTIME_CMD"
  export CONTAINER_RUNTIME_CMD
}

# Abstracted container command wrappers
_containerCmd() {
  $CONTAINER_RUNTIME_CMD "$@"
}

_containerNetwork() {
  local action=$1
  shift
  
  case $action in
    create)
      # Both Docker and Podman support --driver=bridge
      $CONTAINER_RUNTIME_CMD network create --driver=bridge "$@"
      ;;
    list|ls)
      $CONTAINER_RUNTIME_CMD network list "$@"
      ;;
    rm|remove)
      $CONTAINER_RUNTIME_CMD network rm "$@"
      ;;
    *)
      $CONTAINER_RUNTIME_CMD network "$action" "$@"
      ;;
  esac
}

_containerPull() {
  $CONTAINER_RUNTIME_CMD pull "$@"
}

_containerBuild() {
  $CONTAINER_RUNTIME_CMD build "$@"
}

_containerRun() {
  $CONTAINER_RUNTIME_CMD run "$@"
}

_containerPs() {
  $CONTAINER_RUNTIME_CMD ps "$@"
}

_containerExec() {
  $CONTAINER_RUNTIME_CMD exec "$@"
}

_containerCp() {
  $CONTAINER_RUNTIME_CMD cp "$@"
}

_containerRm() {
  $CONTAINER_RUNTIME_CMD rm "$@"
}
