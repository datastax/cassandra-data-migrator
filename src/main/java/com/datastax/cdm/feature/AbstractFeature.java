package com.datastax.cdm.feature;

import com.datastax.cdm.schema.CqlTable;

public abstract class AbstractFeature implements Feature {

    protected boolean isEnabled = false;
    protected boolean isValid = true;
    protected boolean isLoaded = false;

    public AbstractFeature() { }

    @Override
    public boolean isEnabled() {
        if (!isLoaded) throw new RuntimeException("Feature not initialized");
        return isEnabled;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (!isLoaded) throw new RuntimeException("Feature not initialized");
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }
        return isValid;
    }

    /**
     * Validate the properties of the feature typically called by loadProperties as well as initializeAndValidate.
     * It should set isValid to false if any properties are invalid, and ideally uses logger to inform the
     * user of the problems found in property configuration.
     * @return true if the properties are valid, false otherwise
     */
    protected boolean validateProperties() {
        return isValid;
    }

    @Override
    public String toString() {
        return String.format("%s{loaded:%s/valid:%s/enabled:%s}", this.getClass().getSimpleName(), isLoaded, isValid, isEnabled);
    }

}
