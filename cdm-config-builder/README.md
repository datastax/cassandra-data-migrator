# CDM Config Builder

A React application using the [IBM Carbon Design System](https://carbondesignsystem.com/) that provides a user-friendly interface for generating [`cdm.properties`](../src/resources/cdm.properties) configuration files for the [Cassandra Data Migrator (CDM)](https://github.com/datastax/cassandra-data-migrator).

## Features

- **Schema-aware configuration** — paste CQL `CREATE TABLE` statements and the app parses keyspace/table names, primary keys, clustering keys, and column types automatically
- **Best-practices engine** — applies CDM performance recommendations based on estimated row count, table size, and data types present
- **Advanced features** — configure ExplodeMap, ConstantColumns, and ExtractJson transformations via guided form fields
- **Live preview** — the generated `cdm.properties` file updates in real time as you fill out the form
- **Inline comments** — every generated property includes an explanation of why it was set to that value
- **Download & Copy** — save the file as `cdm.properties` or copy to clipboard with one click

## Prerequisites

- **Node.js** 18+ (LTS recommended)
- **npm** 9+

## Getting Started

```bash
# From the repository root
cd cdm-config-builder

# Install dependencies
npm install

# Start the development server
npm run dev
```

The app will be available at **http://localhost:5173** (or the next available port).

## Build for Production

```bash
npm run build
# Output is in cdm-config-builder/dist/
```

To preview the production build locally:

```bash
npm run preview
```

## Project Structure

```
cdm-config-builder/
├── index.html
├── package.json
├── vite.config.js
├── README.md
└── src/
    ├── main.jsx                          # React entry point
    ├── App.jsx                           # Root component, form state, layout
    ├── App.scss                          # Carbon + custom styles
    ├── components/
    │   ├── FormSection.jsx               # Reusable Carbon Tile section wrapper
    │   ├── SchemaSection.jsx             # CQL CREATE TABLE inputs
    │   ├── ConnectionSection.jsx         # Origin/target host, port, SCB, credentials
    │   ├── PerformanceHintsSection.jsx   # Row count, table size, data types, toggles
    │   ├── AdvancedFeaturesSection.jsx   # ExplodeMap, ConstantColumns, ExtractJson
    │   └── PropertiesPreview.jsx         # Live preview panel + Download/Copy buttons
    └── utils/
        ├── parseCqlSchema.js             # CQL DDL parser
        ├── bestPracticesRules.js         # Performance tuning rules engine
        └── generateProperties.js         # cdm.properties file generator
    └── test/
        ├── ConnectionSection.test.jsx
        ├── bestPracticesRules.test.js
        ├── generateProperties.test.js
        ├── parseCqlSchema.test.js
        └── setup.js
```

## How It Works

### 1. Schema Parsing
Paste a full `CREATE TABLE` statement (including keyspace name and `CLUSTERING ORDER BY`) into the **Origin** or **Target** schema fields. The parser extracts:
- Keyspace and table name → sets `spark.cdm.schema.origin.keyspaceTable`
- Partition and clustering key columns → informs `batchSize` recommendations
- Column types → detects BLOBs, counters, collections, UDTs, timestamps

### 2. Best Practices Engine
The rules engine (`bestPracticesRules.js`) evaluates your inputs and applies CDM best practices:

| Input | Effect |
|-------|--------|
| Table size (GB) | Calculates `numParts` = size × 1024 ÷ 10MB per part |
| Row count > 100M | Increases `numParts` to ≥ 50,000 |
| LOBs present | Sets `batchSize=1`, reduces `fetchSizeInRows` to 100 |
| PK = partition key | Sets `batchSize=1` (no clustering, each row is its own partition) |
| Collections-only non-PK | Enables `ttlwritetime.calc.useCollections=true` |
| Counter table | Adds warning comment for `autocorrect.missing.counter` |
| Table > 1TB | Adds Spark cluster recommendation comment |

### 3. Generated Properties
The output includes all standard CDM properties with:
- Inline `#` comments explaining each value
- Commented-out optional properties for easy reference
- Sections matching the structure of `cdm-detailed.properties`

## CDM Reference

- [CDM Properties Reference](../src/resources/cdm-detailed.properties)
- [CDM README](../README.md)
- [CDM GitHub](https://github.com/datastax/cassandra-data-migrator)