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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;

public class ClusterConfigurationExceptionTest {

    @Test
    void testExceptionConstructor() {
        String errorMessage = "Test error message";
        ClusterConfigurationException exception = new ClusterConfigurationException(errorMessage);
        assertEquals(errorMessage, exception.getMessage());

        Exception cause = new RuntimeException("Test cause");
        ClusterConfigurationException exceptionWithCause = new ClusterConfigurationException(errorMessage, cause);
        assertEquals(errorMessage, exceptionWithCause.getMessage());
        assertEquals(cause, exceptionWithCause.getCause());
    }
}