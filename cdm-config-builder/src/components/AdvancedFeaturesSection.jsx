import { Form, TextInput, Toggle, Stack } from '@carbon/react';
import { FormSection } from './FormSection.jsx';

/**
 * Reusable labeled sub-group with a toggle to enable/disable a feature block.
 */
function FeatureBlock({ id, title, description, enabled, onToggle, children }) {
  return (
    <Stack gap={4}>
      <Toggle
        id={`${id}-toggle`}
        labelText={title}
        labelA="Disabled"
        labelB="Enabled"
        toggled={!!enabled}
        onToggle={onToggle}
      />
      {description && !enabled && <p className="feature-block__description">{description}</p>}
      {enabled && <div className="feature-block__fields">{children}</div>}
    </Stack>
  );
}

/**
 * AdvancedFeaturesSection — ExplodeMap, ConstantColumns, ExtractJson configuration.
 *
 * Props:
 *   values   {object}   - flat form state
 *   onChange {function} - (fieldName, value) => void
 */
export function AdvancedFeaturesSection({ values, onChange }) {
  const v = (key) => values[key] ?? '';
  const handle = (key) => (e) => onChange(key, e.target?.value ?? e);

  return (
    <FormSection
      title="Advanced Features"
      description="Optional schema transformation features. Enable only what applies to your migration."
    >
      <Form>
        <Stack gap={7}>
          {/* ── ExplodeMap ─────────────────────────────────────────────── */}
          <FeatureBlock
            id="explode-map"
            title="ExplodeMap — expand MAP column into multiple target rows"
            description="Converts each entry in a MAP column in origin into a separate row in target. The map key and value become separate target columns."
            enabled={values.explodeMapEnabled}
            onToggle={(v) => onChange('explodeMapEnabled', v)}
          >
            <Stack gap={4}>
              <TextInput
                id="explode-map-origin-column"
                labelText="Origin MAP column name"
                helperText="The MAP column in the origin table to explode."
                placeholder="e.g. attributes"
                value={v('explodeMapOriginColumn')}
                onChange={handle('explodeMapOriginColumn')}
              />
              <TextInput
                id="explode-map-target-key"
                labelText="Target key column name"
                helperText="Target column that holds the map key (must be part of target primary key)."
                placeholder="e.g. attr_key"
                value={v('explodeMapTargetKeyColumn')}
                onChange={handle('explodeMapTargetKeyColumn')}
              />
              <TextInput
                id="explode-map-target-value"
                labelText="Target value column name"
                helperText="Target column that holds the map value."
                placeholder="e.g. attr_value"
                value={v('explodeMapTargetValueColumn')}
                onChange={handle('explodeMapTargetValueColumn')}
              />
            </Stack>
          </FeatureBlock>

          {/* ── ConstantColumns ────────────────────────────────────────── */}
          <FeatureBlock
            id="constant-columns"
            title="ConstantColumns — inject fixed values into target columns"
            description="Adds constant (hard-coded) values to target columns that do not exist in origin. Useful for adding metadata or version columns."
            enabled={values.constantColumnsEnabled}
            onToggle={(v) => onChange('constantColumnsEnabled', v)}
          >
            <Stack gap={4}>
              <TextInput
                id="constant-columns-names"
                labelText="Column names (comma-separated)"
                helperText="Target column names to inject."
                placeholder="e.g. source_system,migration_version"
                value={v('constantColumnsNames')}
                onChange={handle('constantColumnsNames')}
              />
              <TextInput
                id="constant-columns-types"
                labelText="Column CQL types (comma-separated)"
                helperText="CQL type for each column in the same order."
                placeholder="e.g. text,int"
                value={v('constantColumnsTypes')}
                onChange={handle('constantColumnsTypes')}
              />
              <TextInput
                id="constant-columns-values"
                labelText="Column values (comma-separated, CQL syntax)"
                helperText="Use CQL literal syntax: 'string', 1234, true."
                placeholder="e.g. 'legacy_system',1"
                value={v('constantColumnsValues')}
                onChange={handle('constantColumnsValues')}
              />
            </Stack>
          </FeatureBlock>

          {/* ── ExtractJson ────────────────────────────────────────────── */}
          <FeatureBlock
            id="extract-json"
            title="ExtractJson — extract a JSON property into a target column"
            description="Reads a JSON string from an origin TEXT column and maps a specific JSON property to a target column."
            enabled={values.extractJsonEnabled}
            onToggle={(v) => onChange('extractJsonEnabled', v)}
          >
            <Stack gap={4}>
              <TextInput
                id="extract-json-origin-column"
                labelText="Origin JSON column name"
                helperText="Origin TEXT column containing JSON content."
                placeholder="e.g. metadata_json"
                value={v('extractJsonOriginColumn')}
                onChange={handle('extractJsonOriginColumn')}
              />
              <TextInput
                id="extract-json-property-mapping"
                labelText="Property mapping (json_property:target_column)"
                helperText="Maps a JSON property name to a target column name."
                placeholder="e.g. user_id:target_user_id"
                value={v('extractJsonPropertyMapping')}
                onChange={handle('extractJsonPropertyMapping')}
              />
              <Toggle
                id="extract-json-exclusive"
                labelText="Exclusive mode (process only mapped columns)"
                labelA="Off"
                labelB="On"
                toggled={!!values.extractJsonExclusive}
                onToggle={(val) => onChange('extractJsonExclusive', val)}
              />
            </Stack>
          </FeatureBlock>
        </Stack>
      </Form>
    </FormSection>
  );
}
