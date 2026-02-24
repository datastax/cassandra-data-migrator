# Usage Examples

## Default (auto-detect runtime engine, Podman preferred)
```
make setup
make test
```

## Explicit Podman
```
CONTAINER_RUNTIME=podman make setup
make test_podman
```

## Explicit Docker
```
CONTAINER_RUNTIME=docker make setup
make test_docker
```

## Script-level
```
./environment.sh -m setup -j ../target/cassandra-data-migrator*.jar -r podman
```

---
