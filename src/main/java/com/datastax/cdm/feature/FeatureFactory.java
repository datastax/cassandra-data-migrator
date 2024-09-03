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

public class FeatureFactory {
    public static Feature getFeature(Featureset feature) {
        switch (feature) {
        case ORIGIN_FILTER:
            return new OriginFilterCondition();
        case CONSTANT_COLUMNS:
            return new ConstantColumns();
        case EXPLODE_MAP:
            return new ExplodeMap();
        case EXTRACT_JSON:
            return new ExtractJson();
        case WRITETIME_TTL:
            return new WritetimeTTL();
        case GUARDRAIL_CHECK:
            return new Guardrail();
        default:
            throw new IllegalArgumentException("Unknown feature: " + feature);
        }
    }

    public static Boolean isEnabled(Feature f) {
        return null != f && f.isEnabled();
    }
}
