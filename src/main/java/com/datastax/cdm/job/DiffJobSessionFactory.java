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
package com.datastax.cdm.job;

import java.io.Serializable;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;

public class DiffJobSessionFactory implements IJobSessionFactory<PartitionRange>, Serializable {
    private static final long serialVersionUID = -3543616512495020278L;
    private static DiffJobSession jobSession = null;

    public AbstractJobSession<PartitionRange> getInstance(CqlSession originSession, CqlSession targetSession,
            PropertyHelper propHelper) {
        if (jobSession == null) {
            synchronized (DiffJobSession.class) {
                if (jobSession == null) {
                    jobSession = new DiffJobSession(originSession, targetSession, propHelper);
                }
            }
        }
        return jobSession;
    }

}
