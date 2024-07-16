/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.job

import java.util

abstract class BasePKJob extends BaseJob[SplitPartitions.PKRows] {
  override def getParts(pieces: Int): util.Collection[SplitPartitions.PKRows] = {
    // This takes a file with N rows and divides it into pieces of size N/pieces
    // Each PKRows object contains a list of Strings that contain the PK to be parsed
    SplitPartitions.getRowPartsFromFile(pieces, this.partitionFileNameInput)
  }

  def execute(): Unit = {
    if (!parts.isEmpty()) {
      slices.foreach(slice => {
        originConnection.withSessionDo(originSession =>
          targetConnection.withSessionDo(targetSession =>
            jobFactory.getInstance(originSession, targetSession, sc)
              .processSlice(slice)))
      })
    }
  }
}