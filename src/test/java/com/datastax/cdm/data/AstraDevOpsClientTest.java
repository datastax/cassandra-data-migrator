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
package com.datastax.cdm.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class AstraDevOpsClientTest {

    @Mock
    private IPropertyHelper propertyHelper;

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse<String> httpResponse;

    private AstraDevOpsClient client;

    @BeforeEach
    void setUp() throws Exception {
        // Create the client with mocked property helper
        client = new AstraDevOpsClient(propertyHelper);

        // Use reflection to replace httpClient with our mock
        Field httpClientField = AstraDevOpsClient.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(client, httpClient);
    }

    @Test
    void testDownloadSecureBundleWithNullToken() throws IOException {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN)).thenReturn(null);

        // Test
        String result = client.downloadSecureBundle(PKFactory.Side.ORIGIN);

        // Verify
        assertNull(result);
        verify(propertyHelper).getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN);
    }

    @Test
    void testDownloadSecureBundleWithEmptyToken() throws IOException {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN)).thenReturn("");

        // Test
        String result = client.downloadSecureBundle(PKFactory.Side.ORIGIN);

        // Verify
        assertNull(result);
        verify(propertyHelper).getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN);
    }

    @Test
    void testDownloadSecureBundleWithNullDatabaseId() throws IOException {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn(null);

        // Test
        String result = client.downloadSecureBundle(PKFactory.Side.ORIGIN);

        // Verify
        assertNull(result);
        verify(propertyHelper).getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN);
        verify(propertyHelper).getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID);
    }

    @Test
    void testGetAstraToken() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_TOKEN)).thenReturn("test-token");

        // Use reflection to access the private method
        Method getAstraTokenMethod = AstraDevOpsClient.class.getDeclaredMethod("getAstraToken", PKFactory.Side.class);
        getAstraTokenMethod.setAccessible(true);

        // Test
        String token = (String) getAstraTokenMethod.invoke(client, PKFactory.Side.ORIGIN);

        // Verify
        assertEquals("test-token", token);
    }

    @Test
    void testGetAstraDatabaseId() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_DATABASE_ID)).thenReturn("test-db-id");

        // Use reflection to access the private method
        Method getAstraDatabaseIdMethod = AstraDevOpsClient.class.getDeclaredMethod("getAstraDatabaseId", PKFactory.Side.class);
        getAstraDatabaseIdMethod.setAccessible(true);

        // Test
        String dbId = (String) getAstraDatabaseIdMethod.invoke(client, PKFactory.Side.TARGET);

        // Verify
        assertEquals("test-db-id", dbId);
    }

    @Test
    void testGetScbType() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_TYPE)).thenReturn("region");

        // Use reflection to access the private method
        Method getScbTypeMethod = AstraDevOpsClient.class.getDeclaredMethod("getScbType", PKFactory.Side.class);
        getScbTypeMethod.setAccessible(true);

        // Test
        String scbType = (String) getScbTypeMethod.invoke(client, PKFactory.Side.ORIGIN);

        // Verify
        assertEquals("region", scbType);
    }

    @Test
    void testGetRegion() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_REGION)).thenReturn("us-east-1");

        // Use reflection to access the private method
        Method getRegionMethod = AstraDevOpsClient.class.getDeclaredMethod("getRegion", PKFactory.Side.class);
        getRegionMethod.setAccessible(true);

        // Test
        String region = (String) getRegionMethod.invoke(client, PKFactory.Side.TARGET);

        // Verify
        assertEquals("us-east-1", region);
    }

    @Test
    void testGetCustomDomain() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_CUSTOM_DOMAIN)).thenReturn("custom.domain.com");

        // Use reflection to access the private method
        Method getCustomDomainMethod = AstraDevOpsClient.class.getDeclaredMethod("getCustomDomain", PKFactory.Side.class);
        getCustomDomainMethod.setAccessible(true);

        // Test
        String customDomain = (String) getCustomDomainMethod.invoke(client, PKFactory.Side.ORIGIN);

        // Verify
        assertEquals("custom.domain.com", customDomain);
    }

    @Test
    void testExtractDownloadUrlDefaultType() throws Exception {
        // Setup
        String jsonResponse = "{ \"downloadURL\": \"https://example.com/bundle.zip\" }";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "default", PKFactory.Side.ORIGIN);

        // Verify
        assertEquals("https://example.com/bundle.zip", url);
    }

    // More tests for extractDownloadUrl with different SCB types can be added here
}