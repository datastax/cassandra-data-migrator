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
package com.datastax.cdm.schema;

/**
 * Exception thrown when there are issues with the Cassandra cluster configuration, such as token overlap problems or
 * other metadata-related issues.
 */
public class ClusterConfigurationException extends RuntimeException {

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message
     *            the detail message
     */
    public ClusterConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause of the exception
     */
    public ClusterConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}