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

import com.datastax.cdm.schema.CqlTable;

public abstract class AbstractFeature implements Feature {

    protected boolean isEnabled = false;
    protected boolean isValid = true;
    protected boolean isLoaded = false;

    public AbstractFeature() {
    }

    @Override
    public boolean isEnabled() {
        if (!isLoaded)
            throw new RuntimeException("Feature not initialized");
        return isEnabled;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (!isLoaded)
            throw new RuntimeException("Feature not initialized");
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }
        return isValid;
    }

    /**
     * Validate the properties of the feature typically called by loadProperties as well as initializeAndValidate. It
     * should set isValid to false if any properties are invalid, and ideally uses logger to inform the user of the
     * problems found in property configuration.
     *
     * @return true if the properties are valid, false otherwise
     */
    protected boolean validateProperties() {
        return isValid;
    }

    @Override
    public String toString() {
        return String.format("%s{loaded:%s/valid:%s/enabled:%s}", this.getClass().getSimpleName(), isLoaded, isValid,
                isEnabled);
    }

}
