/**
 * Parses a CQL CREATE TABLE statement and extracts schema metadata.
 * Supports: keyspace.table, partition keys, clustering keys, column types,
 * CLUSTERING ORDER BY, and detects special column types.
 *
 * @param {string} cql - Full CQL CREATE TABLE statement
 * @returns {object} Parsed schema metadata
 */
export function parseCqlSchema(cql) {
  if (!cql || !cql.trim()) {
    return null;
  }

  const result = {
    keyspace: '',
    table: '',
    keyspaceTable: '',
    columns: [],           // [{ name, type }]
    partitionKeys: [],     // column names
    clusteringKeys: [],    // column names
    allPrimaryKeys: [],    // partition + clustering
    hasCollections: false,
    hasUDTs: false,
    hasCounters: false,
    hasBlobs: false,
    hasTimestamps: false,
    hasNumerics: false,
    hasFrozen: false,
    isPartitionKeyOnly: false, // PK == partition key (no clustering)
    parseError: null,
  };

  try {
    const normalized = cql.replace(/\s+/g, ' ').trim();

    // Extract keyspace.table name
    const tableMatch = normalized.match(
      /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(["`]?[\w]+["`]?)\.(["`]?[\w]+["`]?)\s*\(/i
    );
    if (!tableMatch) {
      result.parseError = 'Could not find CREATE TABLE <keyspace>.<table> pattern.';
      return result;
    }

    result.keyspace = tableMatch[1].replace(/["`]/g, '');
    result.table = tableMatch[2].replace(/["`]/g, '');
    result.keyspaceTable = `${result.keyspace}.${result.table}`;

    // Extract the body between the outermost parentheses
    const bodyStart = normalized.indexOf('(');
    const bodyEnd = findMatchingParen(normalized, bodyStart);
    if (bodyStart === -1 || bodyEnd === -1) {
      result.parseError = 'Could not parse table body.';
      return result;
    }

    const body = normalized.slice(bodyStart + 1, bodyEnd);

    // Split body into top-level clauses (respecting nested parens)
    const clauses = splitTopLevel(body);

    const columnDefs = [];
    let primaryKeyClause = null;

    for (const clause of clauses) {
      const trimmed = clause.trim();
      if (/^PRIMARY\s+KEY\s*\(/i.test(trimmed)) {
        primaryKeyClause = trimmed;
      } else if (/^\w/.test(trimmed)) {
        // Column definition: name type [options]
        const colMatch = trimmed.match(/^(["`]?[\w]+["`]?)\s+(.+)$/i);
        if (colMatch) {
          const colName = colMatch[1].replace(/["`]/g, '');
          const colType = colMatch[2].replace(/\s+PRIMARY\s+KEY.*/i, '').trim();
          columnDefs.push({ name: colName, type: colType });

          // Detect inline PRIMARY KEY (single partition key)
          if (/PRIMARY\s+KEY/i.test(colMatch[2])) {
            result.partitionKeys = [colName];
          }
        }
      }
    }

    result.columns = columnDefs;

    // Parse PRIMARY KEY clause if present
    if (primaryKeyClause) {
      const pkBody = primaryKeyClause.replace(/^PRIMARY\s+KEY\s*/i, '').trim();
      const pkInner = pkBody.slice(1, pkBody.length - 1); // remove outer parens

      // Partition key: may be wrapped in parens for composite
      const partitionMatch = pkInner.match(/^\s*\(([^)]+)\)/);
      if (partitionMatch) {
        result.partitionKeys = partitionMatch[1]
          .split(',')
          .map((s) => s.trim().replace(/["`]/g, ''));
        const rest = pkInner.slice(partitionMatch[0].length);
        result.clusteringKeys = rest
          .split(',')
          .map((s) => s.trim().replace(/["`]/g, ''))
          .filter(Boolean);
      } else {
        // Single partition key, possibly with clustering
        const parts = pkInner.split(',').map((s) => s.trim().replace(/["`]/g, ''));
        result.partitionKeys = [parts[0]];
        result.clusteringKeys = parts.slice(1);
      }
    }

    result.allPrimaryKeys = [...result.partitionKeys, ...result.clusteringKeys];
    result.isPartitionKeyOnly = result.clusteringKeys.length === 0;

    // Detect special column types
    for (const col of columnDefs) {
      const t = col.type.toLowerCase();
      if (t.includes('blob')) result.hasBlobs = true;
      if (t.includes('counter')) result.hasCounters = true;
      if (t.includes('timestamp') || t.includes('date') || t.includes('time')) {
        result.hasTimestamps = true;
      }
      if (
        t.includes('decimal') ||
        t.includes('double') ||
        t.includes('float') ||
        t.includes('bigint') ||
        t.includes('varint')
      ) {
        result.hasNumerics = true;
      }
      if (t.includes('list<') || t.includes('set<') || t.includes('map<')) {
        result.hasCollections = true;
      }
      if (t.includes('frozen<')) result.hasFrozen = true;
      if (t.includes('udt') || (t.startsWith('<') && !isBuiltinType(t))) {
        result.hasUDTs = true;
      }
    }

    // Detect UDTs: column types that are not built-in and not collections
    for (const col of columnDefs) {
      const t = col.type.toLowerCase().replace(/frozen</g, '').replace(/>/g, '').trim();
      if (!isBuiltinType(t) && !t.includes('<')) {
        result.hasUDTs = true;
      }
    }
  } catch (err) {
    result.parseError = `Parse error: ${err.message}`;
  }

  return result;
}

/**
 * Finds the index of the matching closing parenthesis.
 */
function findMatchingParen(str, openIdx) {
  let depth = 0;
  for (let i = openIdx; i < str.length; i++) {
    if (str[i] === '(') depth++;
    else if (str[i] === ')') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/**
 * Splits a comma-separated string at the top level (ignoring nested parens).
 */
function splitTopLevel(str) {
  const parts = [];
  let depth = 0;
  let current = '';
  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    else if (ch === ',' && depth === 0) {
      parts.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

const BUILTIN_TYPES = new Set([
  'ascii', 'bigint', 'blob', 'boolean', 'counter', 'date', 'decimal',
  'double', 'duration', 'float', 'inet', 'int', 'smallint', 'text',
  'time', 'timestamp', 'timeuuid', 'tinyint', 'uuid', 'varchar', 'varint',
  'list', 'set', 'map', 'tuple', 'frozen',
]);

function isBuiltinType(typeStr) {
  const base = typeStr.split('<')[0].trim().toLowerCase();
  return BUILTIN_TYPES.has(base);
}

