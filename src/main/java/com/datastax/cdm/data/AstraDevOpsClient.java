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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Client for interacting with the Astra DevOps API to download secure connect bundles. This client supports downloading
 * different types of SCBs (default, regional, custom domain).
 */
public class AstraDevOpsClient {
    private static final Logger logger = LoggerFactory.getLogger(AstraDevOpsClient.class.getName());
    private static final String ASTRA_API_BASE_URL = "https://api.astra.datastax.com";
    private static final String SCB_API_PATH = "/v2/databases/%s/secureBundleURL?all=true";
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

    private final IPropertyHelper propertyHelper;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new AstraDevOpsClient instance.
     *
     * @param propertyHelper
     *            The property helper for accessing configuration
     */
    public AstraDevOpsClient(IPropertyHelper propertyHelper) {
        this.propertyHelper = propertyHelper;
        this.httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Downloads a secure connect bundle for the specified side (ORIGIN or TARGET).
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The path to the downloaded SCB file, or null if fails
     *
     * @throws IOException
     *             If an error occurs during the download process
     */
    public String downloadSecureBundle(PKFactory.Side side) throws Exception {
        String token = getAstraToken(side);
        String databaseId = getAstraDatabaseId(side);
        String dbRegion = getRegion(side);
        String scbType = getScbType(side);

        if (token == null || token.isEmpty() || databaseId == null || databaseId.isEmpty()) {
            logger.error("Missing required Astra parameters for {} (token or database ID)", side);
            throw new Exception("Failed to download secure bundle");
        }

        logger.info("Auto-downloading secure connect bundle for {} database ID: {}, type: {}, region: {}", side,
                databaseId, scbType, dbRegion != null ? dbRegion : "default");

        try {
            String jsonResponse = fetchSecureBundleUrlInfo(token, databaseId);

            if (jsonResponse == null) {
                logger.error("Failed to fetch secure bundle URL info for {}", side);
                throw new Exception("Failed to download secure bundle");
            }

            String downloadUrl = extractDownloadUrl(jsonResponse, scbType, side, dbRegion);

            if (downloadUrl == null) {
                logger.error("Could not extract download URL for {} bundle type: {}", side, scbType);
                throw new Exception("Failed to download secure bundle");
            }

            return downloadBundleFile(downloadUrl, side);

        } catch (Exception e) {
            logger.error("Error downloading secure bundle for {}: {}", side, e.getMessage());
            throw new IOException("Failed to download secure bundle", e);
        }
    }

    /**
     * Gets the Astra token for the specified side. Uses the database password as the token for Astra.
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The Astra token
     */
    private String getAstraToken(PKFactory.Side side) {
        String property = PKFactory.Side.ORIGIN.equals(side) ? KnownProperties.CONNECT_ORIGIN_PASSWORD
                : KnownProperties.CONNECT_TARGET_PASSWORD;

        return propertyHelper.getAsString(property);
    }

    /**
     * Gets the Astra database ID for the specified side.
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The Astra database ID
     */
    public String getAstraDatabaseId(PKFactory.Side side) {
        String property = PKFactory.Side.ORIGIN.equals(side) ? KnownProperties.ORIGIN_ASTRA_DATABASE_ID
                : KnownProperties.TARGET_ASTRA_DATABASE_ID;

        return propertyHelper.getAsString(property);
    }

    /**
     * Gets the SCB type for the specified side.
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The SCB type ("default", "region", or "custom")
     */
    private String getScbType(PKFactory.Side side) {
        String property = PKFactory.Side.ORIGIN.equals(side) ? KnownProperties.ORIGIN_ASTRA_SCB_TYPE
                : KnownProperties.TARGET_ASTRA_SCB_TYPE;

        return propertyHelper.getAsString(property);
    }

    /**
     * Gets the region for regional SCBs.
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The region name
     */
    public String getRegion(PKFactory.Side side) {
        String property = PKFactory.Side.ORIGIN.equals(side) ? KnownProperties.ORIGIN_ASTRA_SCB_REGION
                : KnownProperties.TARGET_ASTRA_SCB_REGION;

        return propertyHelper.getAsString(property);
    }

    /**
     * Gets the custom domain for custom SCBs.
     *
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The custom domain
     */
    private String getCustomDomain(PKFactory.Side side) {
        String property = PKFactory.Side.ORIGIN.equals(side) ? KnownProperties.ORIGIN_ASTRA_SCB_CUSTOM_DOMAIN
                : KnownProperties.TARGET_ASTRA_SCB_CUSTOM_DOMAIN;

        return propertyHelper.getAsString(property);
    }

    /**
     * Fetches secure bundle URL information from the Astra DevOps API.
     *
     * @param token
     *            The Astra token
     * @param databaseId
     *            The database ID
     *
     * @return The JSON response as a string
     *
     * @throws IOException
     *             If an error occurs during the API call
     * @throws InterruptedException
     *             If the API call is interrupted
     */
    private String fetchSecureBundleUrlInfo(String token, String databaseId) throws IOException, InterruptedException {
        String apiUrl = ASTRA_API_BASE_URL + String.format(SCB_API_PATH, databaseId);
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(apiUrl))
                .header("Authorization", "Bearer " + token).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody()).timeout(HTTP_TIMEOUT).build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            logger.error("Failed to fetch secure bundle URL. Status code: {}, Response: {}", response.statusCode(),
                    response.body());
            return null;
        }

        return response.body();
    }

    /**
     * Extracts the appropriate download URL from the API response based on the SCB type.
     *
     * @param jsonResponse
     *            The JSON response from the API
     * @param scbType
     *            The SCB type
     * @param side
     *            The side (ORIGIN or TARGET)
     * @param dbRegion
     *            The database region
     *
     * @return The download URL
     *
     * @throws IOException
     *             If an error occurs parsing the response
     */
    private String extractDownloadUrl(String jsonResponse, String scbType, PKFactory.Side side, String dbRegion)
            throws IOException {
        JsonNode rootNode = objectMapper.readTree(jsonResponse);

        if (!rootNode.isArray()) {
            logger.error("Expected array response but got {}", rootNode.getNodeType());
            return null;
        }

        // Find the correct datacenter node based on the region
        JsonNode matchingDatacenter = null;

        // If region is specified, find the datacenter with that region
        if (dbRegion != null && !dbRegion.isEmpty()) {
            for (JsonNode datacenterNode : rootNode) {
                if (datacenterNode.has("region") && dbRegion.equalsIgnoreCase(datacenterNode.get("region").asText())) {
                    matchingDatacenter = datacenterNode;
                    break;
                }
            }

            if (matchingDatacenter == null) {
                logger.error("Could not find datacenter for region: {}", dbRegion);
                return null;
            }
        } else {
            // No specific region, use the first datacenter
            if (rootNode.size() > 0) {
                matchingDatacenter = rootNode.get(0);
            } else {
                logger.error("Response contains no datacenters");
                return null;
            }
        }

        switch (scbType.toLowerCase()) {
        case "default":
            // Default bundle URL extraction - use the matched datacenter
            if (matchingDatacenter.has("downloadURL")) {
                return matchingDatacenter.get("downloadURL").asText();
            }
            logger.error("Could not find default download URL in datacenter");
            break;

        case "custom":
            // Custom domain bundle URL extraction
            String customDomain = getCustomDomain(side);
            if (customDomain == null || customDomain.isEmpty()) {
                logger.error("Custom domain is required for SCB type 'custom' but was not specified");
                return null;
            }

            if (matchingDatacenter.has("customDomainBundles")
                    && matchingDatacenter.get("customDomainBundles").isArray()) {

                for (JsonNode customNode : matchingDatacenter.get("customDomainBundles")) {
                    if (customNode.has("domain") && customDomain.equalsIgnoreCase(customNode.get("domain").asText())
                            && customNode.has("downloadURL")) {

                        return customNode.get("downloadURL").asText();
                    }
                }

                logger.error("Could not find downloadURL for custom domain: {} in the selected region {}", customDomain,
                        dbRegion);
            } else {
                logger.error("No customDomainBundles found in the selected region {}", dbRegion);
            }
            break;

        default:
            logger.error("Unknown SCB type: {}", scbType);
            break;
        }

        return null;
    }

    /**
     * Downloads the secure bundle file from the specified URL.
     *
     * @param downloadUrl
     *            The URL to download from
     * @param side
     *            The side (ORIGIN or TARGET)
     *
     * @return The path to the downloaded file
     *
     * @throws IOException
     *             If an error occurs during download
     * @throws InterruptedException
     *             If the download is interrupted
     */
    private String downloadBundleFile(String downloadUrl, PKFactory.Side side)
            throws IOException, InterruptedException {

        logger.info("Downloading secure bundle from URL: {}", downloadUrl);

        HttpRequest downloadRequest = HttpRequest.newBuilder().uri(URI.create(downloadUrl)).GET()
                .timeout(Duration.ofMinutes(2)).build();

        Path tempDir = Files.createTempDirectory("cdm-scb-" + UUID.randomUUID());
        String fileName = side.toString().toLowerCase() + "-secure-bundle.zip";
        Path filePath = tempDir.resolve(fileName);

        HttpResponse<InputStream> downloadResponse = httpClient.send(downloadRequest,
                HttpResponse.BodyHandlers.ofInputStream());

        if (downloadResponse.statusCode() != 200) {
            throw new IOException("Failed to download secure bundle. Status code: " + downloadResponse.statusCode());
        }

        try (InputStream in = downloadResponse.body(); FileOutputStream out = new FileOutputStream(filePath.toFile())) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }

        // Ensure the file is deleted when the JVM exits
        filePath.toFile().deleteOnExit();

        logger.info("Secure bundle downloaded successfully to: {}", filePath);
        return filePath.toString();
    }
}
