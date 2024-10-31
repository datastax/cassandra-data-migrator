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

#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

/local/cdm.sh -f cdm.txt -s guardrailCheck -d "$workingDir" > cdm.guardrailCheck.out 2>cdm.guardrailCheck.err
/local/cdm-assert.sh -f cdm.guardrailCheck.out -a cdm.guardrailCheck.assert -d "$workingDir"
