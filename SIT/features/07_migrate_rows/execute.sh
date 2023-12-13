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

/local/cdm.sh -f cdm.txt -s migrateDataDefault -d "$workingDir"
/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir"


