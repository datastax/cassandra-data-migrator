#!/bin/bash

MAVEN_BASE_VERSION=3.9
MAVEN_REPO_URL="https://archive.apache.org/dist/maven/maven-3/"

curl -sSL ${MAVEN_REPO_URL} | \
  grep -o "${MAVEN_BASE_VERSION}\.[0-99]*\/" | \
  sort -Vu | \
  tail -n1 | \
  sed 's/\///'
