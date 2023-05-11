package com.datastax.cdm.feature;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;

public interface Feature {

    /**
     * Initializes the feature based on properties
     *
     * @param propertyHelper propertyHelper containing initialized properties
     * @return true if the properties appear to be valid, false otherwise
     */
    public boolean loadProperties(IPropertyHelper propertyHelper);

    /**
     * Indicates if feature is enabled.
     * @return true if the feature is enabled, false otherwise
     * @throws RuntimeException if the feature is not loaded
     */
    public boolean isEnabled();

    /**
     * Using the loaded properties, initializes the feature and validates it against the origin and target tables.
     * This method should be called after loadProperties() and before any other method.
     * @param originCqlTable origin CqlTable
     * @param targetCqlTable target CqlTable
     * @return true if the feature is valid, false otherwise
     * @throws RuntimeException if the feature is not loaded, or there is a problem with the properties relative to the tables.
     */
    public boolean initializeAndValidate(CqlTable originCqlTable, CqlTable targetCqlTable);

}
