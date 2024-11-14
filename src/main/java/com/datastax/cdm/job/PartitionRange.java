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
import java.math.BigInteger;

import com.datastax.cdm.job.IJobSessionFactory.JobType;

public class PartitionRange implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BigInteger min;
    private final BigInteger max;
    protected JobCounter jobCounter;

    public PartitionRange(BigInteger min, BigInteger max, JobType jobType) {
        this.min = min;
        this.max = max;
        jobCounter = new JobCounter(jobType);
    }

    public BigInteger getMin() {
        return min;
    }

    public BigInteger getMax() {
        return max;
    }

    public JobCounter getJobCounter() {
        return jobCounter;
    }

    public String toString() {
        return "Processing partition for token range " + min + " to " + max;
    }
}