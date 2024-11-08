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

import org.apache.spark.util.AccumulatorV2;

import com.datastax.cdm.job.IJobSessionFactory.JobType;

public class CDMMetricsAccumulator extends AccumulatorV2<JobCounter, JobCounter> {

    private static final long serialVersionUID = -4185304101452658315L;
    private JobCounter jobCounter;

    public CDMMetricsAccumulator(JobType jobType) {
        jobCounter = new JobCounter(jobType);
    }

    @Override
    public void add(JobCounter v) {
        jobCounter.add(v);
    }

    @Override
    public AccumulatorV2<JobCounter, JobCounter> copy() {
        return this;
    }

    @Override
    public boolean isZero() {
        return jobCounter.isZero();
    }

    @Override
    public void merge(AccumulatorV2<JobCounter, JobCounter> other) {
        jobCounter.add(other.value());
    }

    @Override
    public void reset() {
        jobCounter.reset();
    }

    @Override
    public JobCounter value() {
        return jobCounter;
    }

}
