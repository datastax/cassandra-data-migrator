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

import java.math.BigInteger;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.DataUtility;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.Guardrail;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;

public abstract class AbstractJobSession<T> extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected EnhancedSession originSession;
    protected EnhancedSession targetSession;
    protected Guardrail guardrailFeature;
    protected JobCounter jobCounter;
    protected Long printStatsAfter;
    protected TrackRun trackRunFeature;
    protected long runId;

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, PropertyHelper propHelper) {
        this(originSession, targetSession, propHelper, false);
    }

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, PropertyHelper propHelper,
            boolean isJobMigrateRowsFromFile) {
        super(propHelper);

        if (originSession == null) {
            return;
        }

        this.printStatsAfter = propertyHelper.getLong(KnownProperties.PRINT_STATS_AFTER);
        if (!propertyHelper.meetsMinimum(KnownProperties.PRINT_STATS_AFTER, printStatsAfter, 1L)) {
            logger.warn(KnownProperties.PRINT_STATS_AFTER + " must be greater than 0.  Setting to default value of "
                    + KnownProperties.getDefaultAsString(KnownProperties.PRINT_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.PRINT_STATS_AFTER,
                    KnownProperties.getDefault(KnownProperties.PRINT_STATS_AFTER));
            printStatsAfter = propertyHelper.getLong(KnownProperties.PRINT_STATS_AFTER);
        }
        this.jobCounter = new JobCounter(printStatsAfter,
                propertyHelper.getBoolean(KnownProperties.PRINT_STATS_PER_PART));

        rateLimiterOrigin = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_RATELIMIT_ORIGIN));
        rateLimiterTarget = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_RATELIMIT_TARGET));

        logger.info("PARAM -- Origin Rate Limit: {}", rateLimiterOrigin.getRate());
        logger.info("PARAM -- Target Rate Limit: {}", rateLimiterTarget.getRate());

        CqlTable cqlTableOrigin, cqlTableTarget = null;
        this.originSession = new EnhancedSession(propertyHelper, originSession, true);
        cqlTableOrigin = this.originSession.getCqlTable();
        cqlTableOrigin.setFeatureMap(featureMap);

        boolean allFeaturesValid = true;
        if (targetSession != null) {
            this.targetSession = new EnhancedSession(propertyHelper, targetSession, false);
            cqlTableTarget = this.targetSession.getCqlTable();
            cqlTableOrigin.setOtherCqlTable(cqlTableTarget);
            cqlTableTarget.setOtherCqlTable(cqlTableOrigin);
            cqlTableTarget.setFeatureMap(featureMap);
            for (Feature f : featureMap.values()) {
                if (!f.initializeAndValidate(cqlTableOrigin, cqlTableTarget)) {
                    allFeaturesValid = false;
                    logger.error("Feature {} is not valid.  Please check the configuration.", f.getClass().getName());
                }
            }
        }

        if (!allFeaturesValid) {
            throw new RuntimeException("One or more features are not valid.  Please check the configuration.");
        }

        PKFactory pkFactory = new PKFactory(propertyHelper, cqlTableOrigin, cqlTableTarget);
        this.originSession.setPKFactory(pkFactory);
        this.targetSession.setPKFactory(pkFactory);
        this.guardrailFeature = (Guardrail) cqlTableOrigin.getFeature(Featureset.GUARDRAIL_CHECK);
    }

    public void processSlice(SplitPartitions.Partition slice, TrackRun trackRunFeature, long runId) {
        this.trackRunFeature = trackRunFeature;
        this.runId = runId;
        this.processSlice(slice.getMin(), slice.getMax());
    }

    protected abstract void processSlice(BigInteger min, BigInteger max);

    public synchronized void initCdmRun(long runId, long prevRunId, Collection<SplitPartitions.Partition> parts,
            TrackRun trackRunFeature, TrackRun.RUN_TYPE runType) {
        this.runId = runId;
        this.trackRunFeature = trackRunFeature;
        if (null != trackRunFeature)
            trackRunFeature.initCdmRun(runId, prevRunId, parts, runType);
        DataUtility.deleteGeneratedSCB(runId);
    }

    public synchronized void printCounts(boolean isFinal) {
        if (isFinal) {
            jobCounter.printFinal(runId, trackRunFeature);
        } else {
            jobCounter.printProgress();
        }
    }
}
