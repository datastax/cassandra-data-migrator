#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

MAVEN_BASE_VERSION=3.9
MAVEN_REPO_URL="https://archive.apache.org/dist/maven/maven-3/"

curl -sSL ${MAVEN_REPO_URL} | \
  grep -o "${MAVEN_BASE_VERSION}\.[0-99]*\/" | \
  sort -Vu | \
  tail -n1 | \
  sed 's/\///'
