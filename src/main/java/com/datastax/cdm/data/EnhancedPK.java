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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.ExplodeMap;

public class EnhancedPK {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();

    private final PKFactory factory;
    private final List<Object> values;
    private final List<Class> classes;
    private final Long writeTimestamp;
    private final Integer ttl;
    private boolean errorState = false;
    private boolean warningState = false;
    private List<String> messages;

    private Map<Object, Object> explodeMap;
    private final Object explodeMapKey;
    private final Object explodeMapValue;

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp,
            Object explodeMapKey, Object explodeMapValue) {
        if (logDebug) {
            logger.debug("EnhancedPK: values={}, ttl={}, writeTimestamp={}, explodeMapKey={}, explodeMapValue={}",
                    values, ttl, writeTimestamp, explodeMapKey, explodeMapValue);
        }
        this.factory = factory;
        this.values = (null == explodeMapValue ? values : new ArrayList<>(values)); // copy the list when we will modify
                                                                                    // it
        this.classes = classes;
        this.messages = null;
        this.writeTimestamp = writeTimestamp;
        this.ttl = ttl;
        this.explodeMapKey = explodeMapKey;
        this.explodeMapValue = explodeMapValue;

        if (null != explodeMapKey) {
            this.values.set(factory.getExplodeMapTargetPKIndex(), explodeMapKey);
        }

        validate();
    }

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp) {
        this(factory, values, classes, ttl, writeTimestamp, null, null);
    }

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp,
            Map<Object, Object> explodeMap) {
        this(factory, values, classes, ttl, writeTimestamp, null, null);
        this.explodeMap = explodeMap;
    }

    public List<EnhancedPK> explode(ExplodeMap explodeMapFeature) {
        if (null == explodeMap || explodeMap.isEmpty()) {
            return Collections.singletonList(this);
        }
        return explodeMapFeature.explode(explodeMap).stream().map(entry -> new EnhancedPK(factory, values, classes, ttl,
                writeTimestamp, entry.getKey(), entry.getValue())).collect(Collectors.toList());
    }

    public boolean isError() {
        return errorState;
    }

    public boolean isWarning() {
        return warningState;
    }

    public List<Object> getPKValues() {
        return values;
    }

    public String getMessages() {
        return (null == messages) ? "" : String.join("; ", messages);
    }

    public boolean canExplode() {
        return null != explodeMap && !explodeMap.isEmpty();
    }

    public Object getExplodeMapKey() {
        return this.explodeMapKey;
    }

    public Object getExplodeMapValue() {
        return this.explodeMapValue;
    }

    public Long getWriteTimestamp() {
        return this.writeTimestamp;
    }

    public Integer getTTL() {
        return this.ttl;
    }

    private void validate() {
        if (null == values || null == classes || values.isEmpty() || values.size() != classes.size()) {
            if (null == this.messages)
                this.messages = new ArrayList<>();
            this.messages.add("ERROR: types and/or values are null and/or empty, or are not the same size");
            this.errorState = true;
            return;
        }

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (null != value)
                continue;
            if (i == factory.getExplodeMapTargetPKIndex())
                continue; // this is an unexploded PK

            messages.add(String.format("ERROR: Null value for position %d", i));
            errorState = true;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(" %% ");
            sb.append((null == values.get(i)) ? "(null)" : values.get(i));
        }
        String rawPK = sb.toString();
        if (null != explodeMapKey && null != explodeMapValue) {
            return String.format("[%s {%s->%s}]", rawPK, explodeMapKey, explodeMapValue);
        }
        return String.format("[%s]", rawPK);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof EnhancedPK)) {
            return false;
        }
        EnhancedPK other = (EnhancedPK) o;
        return this.values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
