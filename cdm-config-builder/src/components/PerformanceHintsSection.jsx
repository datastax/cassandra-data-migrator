import { Form, NumberInput, MultiSelect, Stack, Toggle } from '@carbon/react';
import { FormSection } from './FormSection.jsx';

const DATA_TYPE_OPTIONS = [
  { id: 'lobs',        label: 'LOBs (BLOB / large TEXT fields)' },
  { id: 'timestamps',  label: 'Timestamps (TIMESTAMP, DATE, TIME)' },
  { id: 'numerics',    label: 'Numerics (DECIMAL, DOUBLE, FLOAT, BIGINT)' },
  { id: 'collections', label: 'Collections (LIST, SET, MAP)' },
  { id: 'udts',        label: 'UDTs (User-Defined Types)' },
  { id: 'counters',    label: 'Counters (counter type columns)' },
  { id: 'frozen',      label: 'Frozen types (frozen<...>)' },
  { id: 'geospatial',  label: 'Geospatial (DSE Point / Polygon / LineString)' },
];

/**
 * PerformanceHintsSection — optional inputs used to tune CDM performance properties.
 *
 * Props:
 *   rowCount          {number}   - Estimated row count
 *   tableSizeGB       {number}   - Estimated table size in GB
 *   dataTypes         {string[]} - Selected data type IDs
 *   autocorrectMissing  {bool}
 *   autocorrectMismatch {bool}
 *   trackRun            {bool}
 *   trackRunAutoRerun   {bool}
 *   onChange          {function} - (fieldName, value) => void
 */
export function PerformanceHintsSection({
  rowCount,
  tableSizeGB,
  dataTypes,
  autocorrectMissing,
  autocorrectMismatch,
  trackRun,
  trackRunAutoRerun,
  onChange,
}) {
  const selectedDataTypes = DATA_TYPE_OPTIONS.filter((o) => dataTypes?.includes(o.id));

  return (
    <FormSection
      title="Performance Hints & Options"
      description="Optional inputs used to generate best-practice performance settings. Leave blank to use defaults."
    >
      <Form>
        <Stack gap={6}>
          <Stack gap={4} orientation="horizontal" className="perf-row">
            <NumberInput
              id="row-count"
              label="Estimated row count"
              helperText="Used to tune numParts and ratelimit."
              min={0}
              value={rowCount ?? ''}
              onChange={(e, { value }) => onChange('rowCount', value || null)}
              placeholder="e.g. 50000000"
              allowEmpty
            />
            <NumberInput
              id="table-size-gb"
              label="Estimated table size (GB)"
              helperText="Used to tune numParts, batchSize, and fetchSize."
              min={0}
              step={1}
              value={tableSizeGB ?? ''}
              onChange={(e, { value }) => onChange('tableSizeGB', value || null)}
              placeholder="e.g. 250"
              allowEmpty
            />
          </Stack>

          <MultiSelect
            id="data-types"
            titleText="Data types present in the table"
            label="Select all that apply…"
            helperText="Affects fetch size, batch size, codec recommendations, and TTL/WRITETIME handling."
            items={DATA_TYPE_OPTIONS}
            itemToString={(item) => item?.label ?? ''}
            initialSelectedItems={selectedDataTypes}
            onChange={({ selectedItems }) =>
              onChange('dataTypes', selectedItems.map((i) => i.id))
            }
          />

          <Stack gap={4}>
            <Toggle
              id="autocorrect-missing"
              labelText="Autocorrect: re-migrate missing rows (DiffData only)"
              labelA="Off"
              labelB="On"
              toggled={!!autocorrectMissing}
              onToggle={(v) => onChange('autocorrectMissing', v)}
            />
            <Toggle
              id="autocorrect-mismatch"
              labelText="Autocorrect: update mismatched rows (DiffData only)"
              labelA="Off"
              labelB="On"
              toggled={!!autocorrectMismatch}
              onToggle={(v) => onChange('autocorrectMismatch', v)}
            />
          </Stack>

          <Stack gap={4}>
            <Toggle
              id="track-run"
              labelText="Enable run tracking (recommended for large tables)"
              labelA="Off"
              labelB="On"
              toggled={!!trackRun}
              onToggle={(v) => onChange('trackRun', v)}
            />
            {trackRun && (
              <Toggle
                id="track-run-auto-rerun"
                labelText="Auto-resume from last incomplete run"
                labelA="Off"
                labelB="On"
                toggled={!!trackRunAutoRerun}
                onToggle={(v) => onChange('trackRunAutoRerun', v)}
              />
            )}
          </Stack>
        </Stack>
      </Form>
    </FormSection>
  );
}

