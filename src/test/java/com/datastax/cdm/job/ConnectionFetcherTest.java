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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.data.AstraDevOpsClient;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;

@MockitoSettings(strictness = Strictness.LENIENT)
public class ConnectionFetcherTest extends CommonMocks {

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    private SparkConf conf;

    @Mock
    private AstraDevOpsClient astraClient;

    private ConnectionFetcher cf;

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables();
        MockitoAnnotations.openMocks(this);

        cf = new ConnectionFetcher(propertyHelper, null);

        // Default values for all tests
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_HOST)).thenReturn("origin_host");
        when(propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_HOST)).thenReturn("target_host");

        // Default - auto-download disabled for both ORIGIN and TARGET by not setting database ID and region
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn(null);
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn(null);
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_DATABASE_ID)).thenReturn(null);
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_REGION)).thenReturn(null);
    }

    @Test
    public void getConnectionDetailsOrigin() {
        ConnectionDetails cd = cf.getConnectionDetails(PKFactory.Side.ORIGIN);
        assertEquals("origin_host", cd.host());
    }

    @Test
    public void getConnectionDetailsTarget() {
        ConnectionDetails cd = cf.getConnectionDetails(PKFactory.Side.TARGET);
        assertEquals("target_host", cd.host());
    }

    @Test
    public void getConnectionDetailsOriginWithAutoDownloadDisabled() throws Exception {
        // Create ConnectionFetcher with mocked AstraDevOpsClient
        cf = new ConnectionFetcher(propertyHelper, astraClient);

        // Test
        cf.getConnectionDetails(PKFactory.Side.ORIGIN);

        // Verify auto-download was not triggered
        verify(astraClient, never()).downloadSecureBundle(any());
    }

    @Test
    public void getConnectionDetailsOriginWithAutoDownloadEnabled() throws Exception {
        // Setup - auto-download enabled by setting both database ID and region
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn("origin-db-id");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn("us-east-1");
        when(astraClient.getAstraDatabaseId(PKFactory.Side.ORIGIN)).thenReturn("origin-db-id");
        when(astraClient.getRegion(PKFactory.Side.ORIGIN)).thenReturn("us-east-1");
        when(astraClient.downloadSecureBundle(PKFactory.Side.ORIGIN)).thenReturn("/path/to/downloaded/bundle.zip");

        // Create ConnectionFetcher with mocked AstraDevOpsClient
        cf = new ConnectionFetcher(propertyHelper, astraClient);

        // Test
        cf.getConnectionDetails(PKFactory.Side.ORIGIN);

        // Verify the SCB path was updated in the property helper
        verify(propertyHelper).setProperty(KnownProperties.CONNECT_ORIGIN_SCB, "file:///path/to/downloaded/bundle.zip");
    }

    @Test
    public void getConnectionDetailsTargetWithAutoDownloadEnabled() throws Exception {
        // Setup - auto-download enabled by setting both database ID and region
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_DATABASE_ID)).thenReturn("target-db-id");
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_REGION)).thenReturn("eu-west-1");
        when(astraClient.getAstraDatabaseId(PKFactory.Side.TARGET)).thenReturn("target-db-id");
        when(astraClient.getRegion(PKFactory.Side.TARGET)).thenReturn("eu-west-1");
        when(astraClient.downloadSecureBundle(PKFactory.Side.TARGET)).thenReturn("/path/to/downloaded/target-bundle.zip");

        // Create ConnectionFetcher with mocked AstraDevOpsClient
        cf = new ConnectionFetcher(propertyHelper, astraClient);

        // Test
        cf.getConnectionDetails(PKFactory.Side.TARGET);

        // Verify the SCB path was updated in the property helper
        verify(propertyHelper).setProperty(KnownProperties.CONNECT_TARGET_SCB, "file:///path/to/downloaded/target-bundle.zip");
    }

    @Test
    public void getConnectionDetailsWithAutoDownloadFailure() throws Exception {
        // Setup - auto-download enabled by setting both database ID and region
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn("origin-db-id");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn("us-east-1");
        when(astraClient.getAstraDatabaseId(PKFactory.Side.ORIGIN)).thenReturn("origin-db-id");
        when(astraClient.getRegion(PKFactory.Side.ORIGIN)).thenReturn("us-east-1");

        // But the download fails
        when(astraClient.downloadSecureBundle(PKFactory.Side.ORIGIN)).thenThrow(new RuntimeException("Download failed"));

        // Create ConnectionFetcher with mocked AstraDevOpsClient
        cf = new ConnectionFetcher(propertyHelper, astraClient);

        // Test - should not throw exception
        ConnectionDetails cd = cf.getConnectionDetails(PKFactory.Side.ORIGIN);

        // Verify we still get the connection details
        assertEquals("origin_host", cd.host());

        // And no property update happened
        verify(propertyHelper, never()).setProperty(any(), any());
    }
}
