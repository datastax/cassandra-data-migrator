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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class RecordTest {

    @Test
    public void enumValues() {
        assertEquals(4, Record.Diff.values().length);
    }

    @Test
    public void recordException() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            new Record(null, null);
        });
        assertTrue(e.getMessage().contains("pk and at least one row must be provided"));
    }

    @Test
    public void recordWithFutureRow() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            new Record(null, null, null);
        });
        assertTrue(e.getMessage().contains("pk and at least one row must be provided"));
    }

}
