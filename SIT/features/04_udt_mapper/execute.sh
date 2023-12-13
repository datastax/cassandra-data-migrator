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

/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir" > cdm.migrateData.out 2>cdm.migrateData.err
/local/cdm-assert.sh -f cdm.migrateData.out -a cdm.migrateData.assert -d "$workingDir"

/local/cdm.sh -f cdm.txt -s validateData -d "$workingDir" > cdm.validateData.out 2>cdm.validateData.err
/local/cdm-assert.sh -f cdm.validateData.out -a cdm.validateData.assert -d "$workingDir"

cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD $CASS_CLUSTER -f $workingDir/breakData.cql > $workingDir/breakData.out 2> $workingDir/breakData.err

/local/cdm.sh -f cdm.txt -s fixData -d "$workingDir" > cdm.fixData.out 2>cdm.fixData.err
/local/cdm-assert.sh -f cdm.fixData.out -a cdm.fixData.assert -d "$workingDir"
