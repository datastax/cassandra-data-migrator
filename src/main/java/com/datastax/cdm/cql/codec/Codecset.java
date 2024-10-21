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
package com.datastax.cdm.cql.codec;

public enum Codecset {
    INT_STRING, DOUBLE_STRING, BIGINT_STRING, BIGINT_BIGINTEGER, DECIMAL_STRING, TIMESTAMP_STRING_MILLIS,
    TIMESTAMP_STRING_FORMAT, POINT_TYPE, POLYGON_TYPE, DATE_RANGE, LINE_STRING, STRING_BLOB, ASCII_BLOB
}
