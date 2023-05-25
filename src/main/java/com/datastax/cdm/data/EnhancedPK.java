package com.datastax.cdm.data;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.properties.KnownProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

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

    private Map<Object,Object> explodeMap;
    private final Object explodeMapKey;
    private final Object explodeMapValue;

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp, Object explodeMapKey, Object explodeMapValue) {
        if (logDebug) {logger.debug("EnhancedPK: values={}, ttl={}, writeTimestamp={}, explodeMapKey={}, explodeMapValue={}", values, ttl, writeTimestamp, explodeMapKey, explodeMapValue);}
        this.factory = factory;
        this.values = (null==explodeMapValue? values : new ArrayList<>(values)); // copy the list when we will modify it
        this.classes = classes;
        this.messages = null;
        this.writeTimestamp = writeTimestamp;
        this.ttl = ttl;
        this.explodeMapKey = explodeMapKey;
        this.explodeMapValue = explodeMapValue;

        if (null!=explodeMapKey) {this.values.set(factory.getExplodeMapTargetPKIndex(), explodeMapKey);}

        validate();
    }

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp) {
        this(factory, values, classes, ttl, writeTimestamp, null, null);
    }

    public EnhancedPK(PKFactory factory, List<Object> values, List<Class> classes, Integer ttl, Long writeTimestamp, Map<Object,Object> explodeMap) {
        this(factory, values, classes, ttl, writeTimestamp, null, null);
        this.explodeMap = explodeMap;
    }

    public List<EnhancedPK> explode(ExplodeMap explodeMapFeature) {
        if (null == explodeMap || explodeMap.isEmpty()) {
            return Collections.singletonList(this);
        }
        return explodeMapFeature.explode(explodeMap).stream()
                .map(entry -> new EnhancedPK(factory, values, classes, ttl, writeTimestamp, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public boolean isError() {return errorState;}
    public boolean isWarning() {return warningState;}
    public List<Object> getPKValues() {return values;}
    public String getMessages() {return (null==messages)? "" : String.join("; ", messages);}
    public boolean canExplode() {return null != explodeMap && !explodeMap.isEmpty();}
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
        if (null==values || null== classes || values.isEmpty() || values.size() != classes.size()) {
            this.messages.add("ERROR: types and/or values are null and/or empty, or are not the same size");
            this.errorState = true;
            return;
        }

        for (int i=0; i<values.size(); i++) {
            Object value = values.get(i);
            if (null != value) continue;
            if (i==factory.getExplodeMapTargetPKIndex()) continue; // this is an unexploded PK

            // This bit of code addresses the fact we cannot currently insert a NULL value
            // into a primary key column. So we replace it with an alternate value, or
            // mark the PK as invalid.
            this.messages = new ArrayList<>();
            Class c = classes.get(i);
            if (Objects.equals(c, String.class)) {
                values.set(i, factory.getDefaultForMissingString());
                messages.add(String.format("WARN: Defaulting null string value to the empty string for position %d", i));
                warningState = true;
            }
            else if (Objects.equals(c, Instant.class)) {
                Long tsReplaceVal = factory.getDefaultForMissingTimestamp();
                if (null != tsReplaceVal) {
                    values.set(i, Instant.ofEpochSecond(tsReplaceVal).toString());
                    messages.add(String.format("WARN: Defaulting null timestamp to %d for position %d", tsReplaceVal, i));
                    warningState = true;
                }
                else {
                    messages.add(String.format("ERROR: Null value for position %d, consider setting %s", i, KnownProperties.TRANSFORM_REPLACE_MISSING_TS));
                    errorState = true;
                }
            }
            else {
                messages.add(String.format("ERROR: Null value for position %d", i));
                errorState = true;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<values.size(); i++) {
            if (i>0) sb.append(" %% ");
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
