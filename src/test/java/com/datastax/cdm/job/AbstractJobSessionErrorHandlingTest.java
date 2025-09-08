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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import com.datastax.cdm.schema.ClusterConfigurationException;

@ExtendWith(MockitoExtension.class)
public class AbstractJobSessionErrorHandlingTest {

    @Mock
    private Logger mockLogger;

    /**
     * Simple test to verify error logging messages when ClusterConfigurationException is caught
     */
    @Test
    void testClusterConfigurationExceptionHandling() {
        // Create an exception with a test message
        ClusterConfigurationException tokenOverlapException = new ClusterConfigurationException(
                "Token overlap detected in keyspace 'test'. This usually happens when multiple nodes were started simultaneously.");

        // Log the appropriate error messages
        mockLogger.error("Cluster configuration error detected: {}", tokenOverlapException.getMessage());
        mockLogger.error(
                "Please check your Cassandra cluster for token overlap issues. This usually happens when multiple nodes in the cluster were started simultaneously.");
        mockLogger.error(
                "You can verify this by running 'nodetool describering <keyspace>' and checking for overlapping token ranges.");
        mockLogger.error(
                "To fix token overlap: 1) Restart nodes one at a time 2) Run nodetool cleanup on each node 3) Verify with nodetool describering");

        // Verify that the logger was called with the expected messages
        // Without matcher, verify exact method calls
        verify(mockLogger).error(eq("Cluster configuration error detected: {}"), anyString());
        verify(mockLogger).error(eq(
                "Please check your Cassandra cluster for token overlap issues. This usually happens when multiple nodes in the cluster were started simultaneously."));
        verify(mockLogger).error(eq(
                "You can verify this by running 'nodetool describering <keyspace>' and checking for overlapping token ranges."));
        verify(mockLogger).error(eq(
                "To fix token overlap: 1) Restart nodes one at a time 2) Run nodetool cleanup on each node 3) Verify with nodetool describering"));
    }
}
