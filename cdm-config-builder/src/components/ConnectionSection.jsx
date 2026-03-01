import {
  Form,
  FormGroup,
  TextInput,
  NumberInput,
  RadioButtonGroup,
  RadioButton,
  Select,
  SelectItem,
  Stack,
  InlineNotification,
} from '@carbon/react';
import { FormSection } from './FormSection.jsx';

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Reusable connection fields for a single cluster (origin or target).
 * Renders either host/port fields or Astra DB fields based on connectionType.
 */
function ClusterConnectionFields({ prefix, label, values, onChange }) {
  const field = (key) => `${prefix}${key}`;
  const val = (key) => values[field(key)] ?? '';
  const handle = (key) => (e) => onChange(field(key), e.target?.value ?? e);

  const connectionType = val('ConnectionType') || 'host';
  const isAstra = connectionType === 'scb';
  const scbMethod = val('ScbMethod') || 'provide';
  const scbType = val('ScbType') || 'default';

  const databaseId = val('AstraDatabaseId');
  const databaseIdInvalid = databaseId !== '' && !UUID_REGEX.test(databaseId);

  return (
    <FormGroup legendText={label}>
      <Stack gap={4}>
        <RadioButtonGroup
          legendText="Connection type"
          name={`${prefix}connectionType`}
          valueSelected={connectionType}
          onChange={(v) => onChange(field('ConnectionType'), v)}
          orientation="horizontal"
        >
          <RadioButton labelText="Host / Port" value="host" id={`${prefix}type-host`} />
          <RadioButton labelText="Astra DB" value="scb" id={`${prefix}type-scb`} />
        </RadioButtonGroup>

        {/* ── Host / Port fields ── */}
        {!isAstra && (
          <Stack gap={4}>
            <TextInput
              id={field('Host')}
              labelText="Host"
              placeholder="localhost"
              value={val('Host')}
              onChange={handle('Host')}
            />
            <NumberInput
              id={field('Port')}
              label="Port"
              min={1}
              max={65535}
              value={val('Port') || 9042}
              onChange={(e, { value }) => onChange(field('Port'), value)}
            />
          </Stack>
        )}

        {/* ── Astra DB: SCB method sub-options ── */}
        {isAstra && (
          <Stack gap={4}>
            <RadioButtonGroup
              legendText="Secure Connect Bundle"
              name={`${prefix}scbMethod`}
              valueSelected={scbMethod}
              onChange={(v) => onChange(field('ScbMethod'), v)}
              orientation="horizontal"
            >
              <RadioButton labelText="Provide SCB" value="provide" id={`${prefix}scb-provide`} />
              <RadioButton labelText="Auto-download SCB" value="auto" id={`${prefix}scb-auto`} />
            </RadioButtonGroup>

            {/* Provide SCB */}
            {scbMethod === 'provide' && (
              <TextInput
                id={field('Scb')}
                labelText="Secure Connect Bundle path"
                placeholder="file:///path/to/secure-connect-bundle.zip"
                value={val('Scb')}
                onChange={handle('Scb')}
                required
              />
            )}

            {/* Auto-download SCB */}
            {scbMethod === 'auto' && (
              <Stack gap={4}>
                <TextInput
                  id={field('AstraDatabaseId')}
                  labelText="Database UUID"
                  placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                  value={databaseId}
                  onChange={handle('AstraDatabaseId')}
                  invalid={databaseIdInvalid}
                  invalidText="Must be a valid UUID (e.g. 550e8400-e29b-41d4-a716-446655440000)"
                  required
                />
                <TextInput
                  id={field('ScbRegion')}
                  labelText="SCB Region"
                  placeholder="us-east-1"
                  value={val('ScbRegion')}
                  onChange={handle('ScbRegion')}
                  required
                />
                <Select
                  id={field('ScbType')}
                  labelText="SCB Type"
                  value={scbType}
                  onChange={(e) => onChange(field('ScbType'), e.target.value)}
                  required
                >
                  <SelectItem value="default" text="default" />
                  <SelectItem value="custom" text="custom" />
                </Select>
                {scbType === 'custom' && (
                  <>
                    <TextInput
                      id={field('ScbCustomDomain')}
                      labelText="SCB Custom Domain"
                      placeholder="your-custom-domain.example.com"
                      value={val('ScbCustomDomain') || 'your-custom-domain.example.com'}
                      onChange={handle('ScbCustomDomain')}
                      required
                    />
                  </>
                )}
              </Stack>
            )}
          </Stack>
        )}

        {/* ── Credentials ── */}
        <TextInput
          id={field('Username')}
          labelText="Username"
          placeholder={isAstra ? 'token' : 'cassandra'}
          value={isAstra ? 'token' : (val('Username') || 'cassandra')}
          onChange={isAstra ? undefined : handle('Username')}
          readOnly={isAstra}
          required
        />
        <TextInput
          id={field('Password')}
          labelText={isAstra ? 'Password (Starting with "AstraCS:...")' : 'Password'}
          placeholder={isAstra ? 'AstraCS:...' : 'cassandra'}
          type="password"
          value={isAstra ? val('Password') : (val('Password') || 'cassandra')}
          onChange={handle('Password')}
          required
        />
      </Stack>
    </FormGroup>
  );
}

/**
 * ConnectionSection — origin and target cluster connection settings.
 *
 * Props:
 *   values   {object}   - flat form state keyed by originXxx / targetXxx field names
 *   onChange {function} - (fieldName, value) => void
 */
export function ConnectionSection({ values, onChange }) {
  return (
    <FormSection
      title="Connection"
      description="Configure origin (source) and target (destination) cluster connections."
    >
      <Form>
        <Stack gap={7}>
          <ClusterConnectionFields
            prefix="origin"
            label="Origin Cluster"
            values={values}
            onChange={onChange}
          />
          <ClusterConnectionFields
            prefix="target"
            label="Target Cluster"
            values={values}
            onChange={onChange}
          />
        </Stack>
      </Form>
    </FormSection>
  );
}

// Made with Bob
