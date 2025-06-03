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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;

@ExtendWith(MockitoExtension.class)
class AstraDevOpsClientTest {

    @Mock
    private IPropertyHelper propertyHelper;

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse<String> httpResponse;

    @Mock
    private HttpResponse<InputStream> httpResponseStream;

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
    void testDownloadSecureBundleWithNullToken() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn(null);

        // Test
        assertThrows(Exception.class, () -> client.downloadSecureBundle(PKFactory.Side.ORIGIN));

        // Verify
        verify(propertyHelper).getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD);
    }

    @Test
    void testDownloadSecureBundleWithEmptyToken() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("");

        // Test
        assertThrows(Exception.class, () -> client.downloadSecureBundle(PKFactory.Side.ORIGIN));

        // Verify
        verify(propertyHelper).getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD);
    }

    @Test
    void testDownloadSecureBundleWithNullDatabaseId() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn(null);

        // Test
        assertThrows(Exception.class, () -> client.downloadSecureBundle(PKFactory.Side.ORIGIN));

        // Verify
        verify(propertyHelper).getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD);
        verify(propertyHelper).getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID);
    }

    @Test
    void testGetAstraToken() throws Exception {
        // Setup
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("test-token");

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
        String jsonResponse = "[{ \"downloadURL\": \"https://example.com/bundle.zip\" }]";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "default", PKFactory.Side.ORIGIN,
                null);

        // Verify
        assertEquals("https://example.com/bundle.zip", url);
    }

    @Test
    void testExtractDownloadUrlRegionType() throws Exception {
        // Setup
        String jsonResponse = "["
                + "{ \"region\": \"us-east-1\", \"downloadURL\": \"https://us-east-1.example.com/bundle.zip\" },"
                + "{ \"region\": \"us-west-2\", \"downloadURL\": \"https://us-west-2.example.com/bundle.zip\" },"
                + "{ \"region\": \"eu-central-1\", \"downloadURL\": \"https://eu-central-1.example.com/bundle.zip\" }"
                + "]";

        String region = "us-west-2";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "default", PKFactory.Side.ORIGIN,
                region);

        // Verify
        assertEquals("https://us-west-2.example.com/bundle.zip", url);
    }

    @Test
    void testExtractDownloadUrlRegionTypeWithMissingRegion() throws Exception {
        // Setup
        String jsonResponse = "["
                + "{ \"region\": \"us-east-1\", \"downloadURL\": \"https://us-east-1.example.com/bundle.zip\" },"
                + "{ \"region\": \"us-west-2\", \"downloadURL\": \"https://us-west-2.example.com/bundle.zip\" }" + "]";

        String region = null; // Missing region

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test - in new implementation, this should return the first datacenter
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "default", PKFactory.Side.ORIGIN,
                region);

        // Verify - we expect it to return the first URL since no region is specified
        assertEquals("https://us-east-1.example.com/bundle.zip", url);
    }

    @Test
    void testExtractDownloadUrlRegionTypeWithNoMatchingRegion() throws Exception {
        // Setup
        String jsonResponse = "["
                + "{ \"region\": \"us-east-1\", \"downloadURL\": \"https://us-east-1.example.com/bundle.zip\" },"
                + "{ \"region\": \"us-west-2\", \"downloadURL\": \"https://us-west-2.example.com/bundle.zip\" }" + "]";

        String region = "ap-south-1"; // Non-matching region

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "default", PKFactory.Side.ORIGIN,
                region);

        // Verify - no matching region should return null
        assertNull(url);
    }

    @Test
    void testExtractDownloadUrlCustomDomainType() throws Exception {
        // Setup
        String jsonResponse = "[{ " + "\"region\": \"us-east-1\","
                + "\"downloadURL\": \"https://example.com/bundle.zip\"," + "\"customDomainBundles\": ["
                + "{ \"domain\": \"db1.example.com\", \"downloadURL\": \"https://db1.example.com/bundle.zip\" },"
                + "{ \"domain\": \"db2.example.com\", \"downloadURL\": \"https://db2.example.com/bundle.zip\" }" + "]}"
                + "]";

        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_CUSTOM_DOMAIN)).thenReturn("db2.example.com");
        String region = "us-east-1";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "custom", PKFactory.Side.TARGET,
                region);

        // Verify
        assertEquals("https://db2.example.com/bundle.zip", url);
    }

    @Test
    void testExtractDownloadUrlCustomDomainTypeWithMissingDomain() throws Exception {
        // Setup
        String jsonResponse = "[{ " + "\"region\": \"us-east-1\","
                + "\"downloadURL\": \"https://example.com/bundle.zip\"," + "\"customDomainBundles\": ["
                + "{ \"domain\": \"db1.example.com\", \"downloadURL\": \"https://db1.example.com/bundle.zip\" },"
                + "{ \"domain\": \"db2.example.com\", \"downloadURL\": \"https://db2.example.com/bundle.zip\" }" + "]}"
                + "]";

        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_CUSTOM_DOMAIN)).thenReturn(null);
        String region = "us-east-1";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test - missing custom domain
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "custom", PKFactory.Side.TARGET,
                region);

        // Verify - no custom domain should return null
        assertNull(url);
    }

    @Test
    void testExtractDownloadUrlUnknownType() throws Exception {
        // Setup
        String jsonResponse = "[{ \"downloadURL\": \"https://example.com/bundle.zip\" }]";

        // Use reflection to access the private method
        Method extractDownloadUrlMethod = AstraDevOpsClient.class.getDeclaredMethod("extractDownloadUrl", String.class,
                String.class, PKFactory.Side.class, String.class);
        extractDownloadUrlMethod.setAccessible(true);

        // Test with unknown SCB type
        String url = (String) extractDownloadUrlMethod.invoke(client, jsonResponse, "unknown", PKFactory.Side.ORIGIN,
                null);

        // Verify
        assertNull(url);
    }

    @Test
    void testFetchSecureBundleUrlInfo() throws Exception {
        // Mock the HTTP response
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("{ \"downloadURL\": \"https://example.com/bundle.zip\" }");

        // Mock the HTTP client to return our mocked response
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofString()))).thenReturn(httpResponse);

        // Use reflection to access the private method
        Method fetchSecureBundleUrlInfoMethod = AstraDevOpsClient.class.getDeclaredMethod(
                "fetchSecureBundleUrlInfo", String.class, String.class);
        fetchSecureBundleUrlInfoMethod.setAccessible(true);

        // Test
        String jsonResponse = (String) fetchSecureBundleUrlInfoMethod.invoke(client, "test-token", "test-db-id");

        // Verify
        assertEquals("{ \"downloadURL\": \"https://example.com/bundle.zip\" }", jsonResponse);

        // Verify the correct URL was used
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), eq(HttpResponse.BodyHandlers.ofString()));

        HttpRequest capturedRequest = requestCaptor.getValue();
        assertEquals(URI.create("https://api.astra.datastax.com/v2/databases/test-db-id/secureBundleURL?all=true"),
                capturedRequest.uri());
        assertTrue(capturedRequest.headers().firstValue("Authorization").isPresent());
        assertEquals("Bearer test-token", capturedRequest.headers().firstValue("Authorization").get());
    }

    @Test
    void testFetchSecureBundleUrlInfoError() throws Exception {
        // Mock the HTTP response for an error
        when(httpResponse.statusCode()).thenReturn(401);
        when(httpResponse.body()).thenReturn("{ \"error\": \"Unauthorized\" }");

        // Mock the HTTP client to return our mocked response
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofString()))).thenReturn(httpResponse);

        // Use reflection to access the private method
        Method fetchSecureBundleUrlInfoMethod = AstraDevOpsClient.class.getDeclaredMethod(
                "fetchSecureBundleUrlInfo", String.class, String.class);
        fetchSecureBundleUrlInfoMethod.setAccessible(true);

        // Test
        String jsonResponse = (String) fetchSecureBundleUrlInfoMethod.invoke(client, "invalid-token", "test-db-id");

        // Verify
        assertNull(jsonResponse);
    }

    @Test
    void testDownloadBundleFile() throws Exception {
        // Setup
        byte[] mockData = new byte[100]; // Mock some binary data
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockData);

        // Mock the HTTP response
        when(httpResponseStream.statusCode()).thenReturn(200);
        when(httpResponseStream.body()).thenReturn(inputStream);

        // Mock the HTTP client to return our mocked response
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofInputStream()))).thenReturn(httpResponseStream);

        // Use reflection to access the private method
        Method downloadBundleFileMethod = AstraDevOpsClient.class.getDeclaredMethod("downloadBundleFile", String.class,
                PKFactory.Side.class);
        downloadBundleFileMethod.setAccessible(true);

        // Test
        String filePath = (String) downloadBundleFileMethod.invoke(client, "https://example.com/bundle.zip",
                PKFactory.Side.ORIGIN);

        // Verify
        assertNotNull(filePath);
        assertTrue(filePath.contains("origin-secure-bundle.zip"));

        // Verify the correct URL was used
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), eq(HttpResponse.BodyHandlers.ofInputStream()));

        HttpRequest capturedRequest = requestCaptor.getValue();
        assertEquals(URI.create("https://example.com/bundle.zip"), capturedRequest.uri());
    }

    @Test
    void testDownloadBundleFileHttpError() throws Exception {
        // Setup

        // Mock the HTTP response with an error status
        when(httpResponseStream.statusCode()).thenReturn(404);

        // Mock the HTTP client to return our mocked response
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofInputStream()))).thenReturn(httpResponseStream);

        // Use reflection to access the private method
        Method downloadBundleFileMethod = AstraDevOpsClient.class.getDeclaredMethod(
                "downloadBundleFile", String.class, PKFactory.Side.class);
        downloadBundleFileMethod.setAccessible(true);

        try {
            downloadBundleFileMethod.invoke(client, "https://example.com/not-found.zip", PKFactory.Side.ORIGIN);
            fail("Expected an exception to be thrown");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Extract the actual exception that was wrapped
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("Failed to download secure bundle. Status code: 404", e.getCause().getMessage());
        }
    }

    @Test
    void testDownloadSecureBundleSuccess() throws Exception {
        // Setup - mock all the components for a successful download

        // Step 1: Mock the API response for fetching the SCB URL
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("[{ \"downloadURL\": \"https://example.com/bundle.zip\" }]");

        // Step 2: Mock the binary download
        byte[] mockData = new byte[100]; // Mock some binary data
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockData);

        // Mock the HTTP response
        when(httpResponseStream.statusCode()).thenReturn(200);
        when(httpResponseStream.body()).thenReturn(inputStream);

        // Configure the HTTP client to return responses based on different handler types
        // We need to use doReturn().when() syntax here to avoid NullPointerException in the matcher
        doReturn(httpResponse).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofString()));
        doReturn(httpResponseStream).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofInputStream()));

        // Mock the property helper
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn("test-db-id");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_TYPE)).thenReturn("default");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn(null);

        // Test
        String filePath = client.downloadSecureBundle(PKFactory.Side.ORIGIN);

        // Verify
        assertNotNull(filePath);
        assertTrue(filePath.contains("origin-secure-bundle.zip"));
    }

    @Test
    void testDownloadBundleFileWithIOException() throws Exception {
        // Mock the HTTP client to throw IOException when sending the request
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofInputStream())))
            .thenThrow(new IOException("Network error"));

        // Use reflection to access the private method
        Method downloadBundleFileMethod = AstraDevOpsClient.class.getDeclaredMethod(
                "downloadBundleFile", String.class, PKFactory.Side.class);
        downloadBundleFileMethod.setAccessible(true);

        try {
            // Test - should throw an IOException
            downloadBundleFileMethod.invoke(client, "https://example.com/bundle.zip", PKFactory.Side.ORIGIN);
            fail("Expected an exception to be thrown");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Extract the actual exception that was wrapped
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("Network error", e.getCause().getMessage());
        }
    }

    @Test
    void testDownloadSecureBundleWithEmptyJsonResponse() throws Exception {
        // Setup
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("[]"); // Empty array response

        // Configure the HTTP client to return our mocked response
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofString()))).thenReturn(httpResponse);

        // Mock the property helper
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn("test-db-id");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_TYPE)).thenReturn("default");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn(null);

        // Test
        assertThrows(Exception.class, () -> client.downloadSecureBundle(PKFactory.Side.ORIGIN));
    }

    @Test
    void testDownloadSecureBundleWithRegionalSCB() throws Exception {
        // Setup - mock all the components for a successful download with regional SCB

        // Step 1: Mock the API response for fetching the SCB URL with regional data
        String jsonResponse = "["
                + "{ \"region\": \"us-east-1\", \"downloadURL\": \"https://us-east-1.example.com/bundle.zip\" },"
                + "{ \"region\": \"us-west-2\", \"downloadURL\": \"https://us-west-2.example.com/bundle.zip\" },"
                + "{ \"region\": \"eu-central-1\", \"downloadURL\": \"https://eu-central-1.example.com/bundle.zip\" }"
                + "]";

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(jsonResponse);

        // Step 2: Mock the binary download
        byte[] mockData = new byte[100]; // Mock some binary data
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockData);

        // Mock the HTTP response
        when(httpResponseStream.statusCode()).thenReturn(200);
        when(httpResponseStream.body()).thenReturn(inputStream);

        // Configure the HTTP client to return responses based on different handler types
        doReturn(httpResponse).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofString()));
        doReturn(httpResponseStream).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofInputStream()));

        // Mock the property helper
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_DATABASE_ID)).thenReturn("test-db-id");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_TYPE)).thenReturn("default");
        when(propertyHelper.getAsString(KnownProperties.ORIGIN_ASTRA_SCB_REGION)).thenReturn("us-west-2");

        // Test
        String filePath = client.downloadSecureBundle(PKFactory.Side.ORIGIN);

        // Verify
        assertNotNull(filePath);
        assertTrue(filePath.contains("origin-secure-bundle.zip"));

        // Verify that the URL request included all=true parameter
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient, atLeastOnce()).send(requestCaptor.capture(), eq(HttpResponse.BodyHandlers.ofString()));

        // Verify the URL includes all=true parameter
        boolean foundAllParam = false;
        for (HttpRequest capturedRequest : requestCaptor.getAllValues()) {
            if (capturedRequest.uri().toString().contains("all=true")) {
                foundAllParam = true;
                break;
            }
        }
        assertTrue(foundAllParam, "The request URL should include all=true parameter for regional SCB");
    }

    @Test
    void testDownloadSecureBundleWithCustomDomainSCB() throws Exception {
        // Setup - mock all the components for a successful download with custom domain SCB

        // Step 1: Mock the API response for fetching the SCB URL with custom domain data
        String jsonResponse = "[{ " + "\"region\": \"us-east-1\","
                + "\"downloadURL\": \"https://example.com/bundle.zip\"," + "\"customDomainBundles\": ["
                + "{ \"domain\": \"db1.example.com\", \"downloadURL\": \"https://db1.example.com/bundle.zip\" },"
                + "{ \"domain\": \"my-custom-domain.example.com\", \"downloadURL\": \"https://my-custom-domain.example.com/bundle.zip\" }"
                + "]}" + "]";

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(jsonResponse);

        // Step 2: Mock the binary download
        byte[] mockData = new byte[100]; // Mock some binary data
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockData);

        // Mock the HTTP response
        when(httpResponseStream.statusCode()).thenReturn(200);
        when(httpResponseStream.body()).thenReturn(inputStream);

        // Configure the HTTP client to return responses based on different handler types
        doReturn(httpResponse).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofString()));
        doReturn(httpResponseStream).when(httpClient).send(any(), eq(HttpResponse.BodyHandlers.ofInputStream()));

        // Mock the property helper
        when(propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_PASSWORD)).thenReturn("test-token");
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_DATABASE_ID)).thenReturn("test-db-id");
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_TYPE)).thenReturn("custom");
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_CUSTOM_DOMAIN))
                .thenReturn("my-custom-domain.example.com");
        when(propertyHelper.getAsString(KnownProperties.TARGET_ASTRA_SCB_REGION)).thenReturn("us-east-1");

        // Test
        String filePath = client.downloadSecureBundle(PKFactory.Side.TARGET);

        // Verify
        assertNotNull(filePath);
        assertTrue(filePath.contains("target-secure-bundle.zip"));
    }

    @Test
    void testDownloadBundleFileInterrupted() throws Exception {
        // Mock the HTTP client to throw InterruptedException when sending the request
        when(httpClient.send(any(), eq(HttpResponse.BodyHandlers.ofInputStream())))
            .thenThrow(new InterruptedException("Download interrupted"));

        // Use reflection to access the private method
        Method downloadBundleFileMethod = AstraDevOpsClient.class.getDeclaredMethod(
                "downloadBundleFile", String.class, PKFactory.Side.class);
        downloadBundleFileMethod.setAccessible(true);

        try {
            // Test - should throw an InterruptedException wrapped in an InvocationTargetException
            downloadBundleFileMethod.invoke(client, "https://example.com/bundle.zip", PKFactory.Side.ORIGIN);
            fail("Expected an exception to be thrown");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Extract the actual exception that was wrapped
            assertTrue(e.getCause() instanceof InterruptedException);
            assertEquals("Download interrupted", e.getCause().getMessage());
        }
    }
}
