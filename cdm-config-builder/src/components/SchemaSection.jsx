import { Form, Stack, TextArea, InlineNotification } from '@carbon/react';
import { FormSection } from './FormSection.jsx';

const CQL_PLACEHOLDER = `CREATE TABLE keyspace_name.table_name (
  id uuid,
  bucket int,
  created_at timestamp,
  data text,
  tags set<text>,
  PRIMARY KEY ((id, bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);`;

/**
 * Renders a labeled CQL TextArea with optional parse error notification.
 */
function CqlInput({ id, label, helperText, value, onChange, parseError }) {
  return (
    <Stack gap={3}>
      <TextArea
        id={id}
        labelText={label}
        helperText={helperText}
        placeholder={CQL_PLACEHOLDER}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        rows={8}
        enableCounter
        maxCount={5000}
      />
      {parseError && (
        <InlineNotification
          kind="warning"
          title="Schema parse warning: "
          subtitle={parseError}
          lowContrast
          hideCloseButton
        />
      )}
    </Stack>
  );
}

/**
 * SchemaSection — CQL CREATE TABLE inputs for origin and target tables.
 *
 * Props:
 *   originCql        {string}   - Raw CQL for origin table
 *   targetCql        {string}   - Raw CQL for target table
 *   originSchema     {object}   - Parsed origin schema (from parseCqlSchema)
 *   targetSchema     {object}   - Parsed target schema (from parseCqlSchema)
 *   onOriginChange   {function} - (cql: string) => void
 *   onTargetChange   {function} - (cql: string) => void
 */
export function SchemaSection({
  originCql,
  targetCql,
  originSchema,
  targetSchema,
  onOriginChange,
  onTargetChange,
}) {
  return (
    <FormSection
      title="Table Schema"
      description="Paste the full CQL CREATE TABLE statements for origin and target tables. Include keyspace name and CLUSTERING ORDER BY if applicable."
    >
      <Form>
        <Stack gap={6}>
          <CqlInput
            id="origin-cql"
            label="Origin Table Schema (required)"
            helperText="Full CREATE TABLE statement including keyspace name."
            value={originCql}
            onChange={onOriginChange}
            parseError={originSchema?.parseError}
          />

          {originSchema && !originSchema.parseError && (
            <div className="schema-badge-row">
              <span className="schema-badge">
                📋 {originSchema.keyspaceTable}
              </span>
              {originSchema.partitionKeys.length > 0 && (
                <span className="schema-badge schema-badge--info">
                  PK: {originSchema.partitionKeys.join(', ')}
                </span>
              )}
              {originSchema.clusteringKeys.length > 0 && (
                <span className="schema-badge schema-badge--info">
                  CK: {originSchema.clusteringKeys.join(', ')}
                </span>
              )}
              {originSchema.hasCounters && (
                <span className="schema-badge schema-badge--warning">⚠ Counter table</span>
              )}
              {originSchema.hasCollections && (
                <span className="schema-badge schema-badge--neutral">Collections</span>
              )}
              {originSchema.hasBlobs && (
                <span className="schema-badge schema-badge--neutral">BLOBs</span>
              )}
            </div>
          )}

          <CqlInput
            id="target-cql"
            label="Target Table Schema (required)"
            helperText="Full CREATE TABLE statement. Leave identical to origin if schema is unchanged."
            value={targetCql}
            onChange={onTargetChange}
            parseError={targetSchema?.parseError}
          />

          {targetSchema && !targetSchema.parseError && targetSchema.keyspaceTable !== originSchema?.keyspaceTable && (
            <div className="schema-badge-row">
              <span className="schema-badge schema-badge--success">
                🎯 {targetSchema.keyspaceTable}
              </span>
            </div>
          )}
        </Stack>
      </Form>
    </FormSection>
  );
}

