import { useMemo, useCallback, useReducer } from 'react';
import {
  Header,
  HeaderName,
  HeaderGlobalBar,
  HeaderGlobalAction,
  Content,
  Grid,
  Column,
  Theme,
} from '@carbon/react';

import { SchemaSection } from './components/SchemaSection.jsx';
import { ConnectionSection } from './components/ConnectionSection.jsx';
import { PerformanceHintsSection } from './components/PerformanceHintsSection.jsx';
import { AdvancedFeaturesSection } from './components/AdvancedFeaturesSection.jsx';
import { PropertiesPreview } from './components/PropertiesPreview.jsx';
import { SunIcon, MoonIcon } from './components/ThemeToggleButton.jsx';
import { ThemeProvider, useTheme } from './context/ThemeContext.jsx';

import { parseCqlSchema } from './utils/parseCqlSchema.js';
import { generateProperties } from './utils/generateProperties.js';

// ── Initial form state ────────────────────────────────────────────────────────
const INITIAL_STATE = {
  // Connection — origin
  originConnectionType: 'host',
  originHost: 'localhost',
  originPort: 9042,
  originUsername: 'cassandra',
  originPassword: 'cassandra',
  originScb: '',

  // Connection — target
  targetConnectionType: 'host',
  targetHost: 'localhost',
  targetPort: 9042,
  targetUsername: 'cassandra',
  targetPassword: 'cassandra',
  targetScb: '',

  // Schema
  originCql: '',
  targetCql: '',

  // Performance hints
  rowCount: null,
  tableSizeGB: null,
  dataTypes: [],

  // Autocorrect
  autocorrectMissing: false,
  autocorrectMismatch: false,

  // Track run
  trackRun: false,
  trackRunAutoRerun: false,

  // Advanced features — ExplodeMap
  explodeMapEnabled: false,
  explodeMapOriginColumn: '',
  explodeMapTargetKeyColumn: '',
  explodeMapTargetValueColumn: '',

  // Advanced features — ConstantColumns
  constantColumnsEnabled: false,
  constantColumnsNames: '',
  constantColumnsTypes: '',
  constantColumnsValues: '',

  // Advanced features — ExtractJson
  extractJsonEnabled: false,
  extractJsonOriginColumn: '',
  extractJsonPropertyMapping: '',
  extractJsonExclusive: false,
};

// ── Reducer ───────────────────────────────────────────────────────────────────
function formReducer(state, { field, value }) {
  return { ...state, [field]: value };
}

// ── Inner App (needs ThemeProvider in scope) ──────────────────────────────────
function AppInner() {
  const { theme, toggleTheme } = useTheme();
  const carbonTheme = theme === 'dark' ? 'g100' : 'g10';

  const [formState, dispatch] = useReducer(formReducer, INITIAL_STATE);

  // Single onChange handler — (fieldName, value) => void
  const handleChange = useCallback((field, value) => {
    dispatch({ field, value });
  }, []);

  // Parse CQL schemas reactively
  const originSchema = useMemo(
    () => (formState.originCql ? parseCqlSchema(formState.originCql) : null),
    [formState.originCql]
  );
  const targetSchema = useMemo(
    () => (formState.targetCql ? parseCqlSchema(formState.targetCql) : null),
    [formState.targetCql]
  );

  // Generate properties file content reactively
  const propertiesContent = useMemo(
    () =>
      generateProperties({
        ...formState,
        originSchema,
        targetSchema,
      }),
     
    [formState, originSchema, targetSchema]
  );

  return (
    <Theme theme={carbonTheme}>
      {/* ── Header ─────────────────────────────────────────────────────── */}
      <Header aria-label="CDM Config Builder">
        <HeaderName prefix="DataStax">CDM Config Builder</HeaderName>
        <HeaderGlobalBar>
          <HeaderGlobalAction
            aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
            tooltipAlignment="end"
            onClick={toggleTheme}
            className="theme-toggle-action"
          >
            {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
          </HeaderGlobalAction>
        </HeaderGlobalBar>
      </Header>

      {/* ── Main content ───────────────────────────────────────────────── */}
      <Content className="app-content">
        <Grid fullWidth className="app-grid">

          {/* ── LEFT PANEL: Form ─────────────────────────────────────── */}
          <Column sm={4} md={8} lg={8} className="form-column">
            <div className="form-stack">

              <SchemaSection
                originCql={formState.originCql}
                targetCql={formState.targetCql}
                originSchema={originSchema}
                targetSchema={targetSchema}
                onOriginChange={(v) => handleChange('originCql', v)}
                onTargetChange={(v) => handleChange('targetCql', v)}
              />

              <ConnectionSection
                values={formState}
                onChange={handleChange}
              />

              <PerformanceHintsSection
                rowCount={formState.rowCount}
                tableSizeGB={formState.tableSizeGB}
                dataTypes={formState.dataTypes}
                autocorrectMissing={formState.autocorrectMissing}
                autocorrectMismatch={formState.autocorrectMismatch}
                trackRun={formState.trackRun}
                trackRunAutoRerun={formState.trackRunAutoRerun}
                onChange={handleChange}
              />

              <AdvancedFeaturesSection
                values={formState}
                onChange={handleChange}
              />

            </div>
          </Column>

          {/* ── RIGHT PANEL: Preview ──────────────────────────────────── */}
          <Column sm={4} md={8} lg={8} className="preview-column">
            <div className="preview-sticky">
              <PropertiesPreview content={propertiesContent} />
            </div>
          </Column>

        </Grid>
      </Content>
    </Theme>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  return (
    <ThemeProvider>
      <AppInner />
    </ThemeProvider>
  );
}

