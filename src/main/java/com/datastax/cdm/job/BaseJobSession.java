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

import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.feature.FeatureFactory;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseJobSession {

    public static final String THREAD_CONTEXT_LABEL = "ThreadLabel";
    protected static final String NEW_LINE = System.lineSeparator();
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected PropertyHelper propertyHelper = PropertyHelper.getInstance();
    protected Map<Featureset, Feature> featureMap;
    protected RateLimiter rateLimiterOrigin;
    protected RateLimiter rateLimiterTarget;
    protected Integer maxRetries = 10;

    protected BaseJobSession(SparkConf sc) {
        propertyHelper.initializeSparkConf(sc);
        this.featureMap = calcFeatureMap(propertyHelper);
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel());
    }

    private Map<Featureset, Feature> calcFeatureMap(PropertyHelper propertyHelper) {
        Map<Featureset, Feature> rtn = new HashMap<>();
        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = FeatureFactory.getFeature(f); // FeatureFactory throws an RTE if the feature is not implemented
            if (feature.loadProperties(propertyHelper)) {
                rtn.put(f, feature);
            }
        }
        return rtn;
    }

    protected String getThreadLabel() {
        return ThreadContext.get("main");
    }

    protected String getThreadLabel(BigInteger min, BigInteger max) {
        String minString = min.toString();
        String maxString = max.toString();
        int minWidth = 20;
        int formattedMaxWidth = Math.max(Math.max(minString.length(), maxString.length()), minWidth);

        String formattedMin = String.format("%-" + minWidth + "s", minString).trim();
        String formattedMax = String.format("%" + formattedMaxWidth + "s", maxString);

        return formattedMin + ":" + formattedMax;
    }

    private void appendToFile(String filePath, String content) throws IOException {
        if (StringUtils.isAllBlank(filePath)) {
            filePath = "./" + propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) + "_partitions.csv";

        }
        Path path = Paths.get(filePath);
        if (StringUtils.isNotBlank(path.getParent().toString())) {
            Files.createDirectories(path.getParent());
        } else {
            path = Paths.get("./" + filePath);
        }

        Files.write(path, (content + NEW_LINE).getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    protected void logPartitionsInFile(String partitionFile, BigInteger min, BigInteger max) {
        try {
            appendToFile(partitionFile, min + "," + max);
        } catch (Exception ee) {
            logger.error("Error occurred while writing to token range file min: {} max: {}", min, max, ee);
        }
    }

}
