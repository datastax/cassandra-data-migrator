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
package com.datastax.cdm.feature;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;

public interface Feature {

    /**
     * Initializes the feature based on properties
     *
     * @param propertyHelper
     *            propertyHelper containing initialized properties
     *
     * @return true if the properties appear to be valid, false otherwise
     */
    public boolean loadProperties(IPropertyHelper propertyHelper);

    /**
     * Indicates if feature is enabled.
     *
     * @return true if the feature is enabled, false otherwise
     *
     * @throws RuntimeException
     *             if the feature is not loaded
     */
    public boolean isEnabled();

    /**
     * Using the loaded properties, initializes the feature and validates it against the origin and target tables. This
     * method should be called after loadProperties() and before any other method.
     *
     * @param originCqlTable
     *            origin CqlTable
     * @param targetCqlTable
     *            target CqlTable
     *
     * @return true if the feature is valid, false otherwise
     *
     * @throws RuntimeException
     *             if the feature is not loaded, or there is a problem with the properties relative to the tables.
     */
    public boolean initializeAndValidate(CqlTable originCqlTable, CqlTable targetCqlTable);

}
