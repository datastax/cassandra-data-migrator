/**
 * Best practices rules engine for CDM configuration.
 *
 * Evaluates form inputs and returns recommended property values with
 * explanatory comments for each decision.
 *
 * @param {object} inputs
 * @param {object|null} inputs.originSchema  - Parsed origin CQL schema
 * @param {object|null} inputs.targetSchema  - Parsed target CQL schema
 * @param {number|null} inputs.rowCount      - Estimated number of rows
 * @param {number|null} inputs.tableSizeGB   - Estimated table size in GB
 * @param {string[]}    inputs.dataTypes     - Selected data type flags
 * @returns {object} { properties: {key: value}, comments: {key: string} }
 */
export function applyBestPractices({ originSchema, rowCount, tableSizeGB, dataTypes = [] }) {
  const props = {};
  const comments = {};

  const hasLOBs = dataTypes.includes('lobs') || originSchema?.hasBlobs;
  const hasTimestamps = dataTypes.includes('timestamps') || originSchema?.hasTimestamps;
  const hasNumerics = dataTypes.includes('numerics') || originSchema?.hasNumerics;
  const hasCollections = dataTypes.includes('collections') || originSchema?.hasCollections;
  const hasUDTs = dataTypes.includes('udts') || originSchema?.hasUDTs;
  const hasCounters = dataTypes.includes('counters') || originSchema?.hasCounters;

  const isPartitionKeyOnly = originSchema?.isPartitionKeyOnly ?? false;
  const sizeGB = tableSizeGB ?? 0;
  const rows = rowCount ?? 0;

  // ── numParts ──────────────────────────────────────────────────────────────
  // Aim for ~10MB per partition. Default assumes ~50GB table (5000 parts).
  let numParts = 5000;
  let numPartsComment = 'Default: assumes ~50GB table (5000 parts × 10MB each).';

  if (sizeGB > 0) {
    numParts = Math.max(1000, Math.ceil((sizeGB * 1024) / 10));
    numPartsComment =
      `Calculated from estimated table size (${sizeGB} GB ÷ 10MB per part = ${numParts} parts). ` +
      'Aim for ~10MB of data per partition for optimal parallelism.';
  } else if (rows > 0) {
    // Rough heuristic: 1M rows ≈ 1GB for average row sizes
    const estimatedGB = rows / 1_000_000;
    numParts = Math.max(1000, Math.ceil((estimatedGB * 1024) / 10));
    numPartsComment =
      `Estimated from row count (${rows.toLocaleString()} rows ≈ ${estimatedGB.toFixed(1)} GB). ` +
      'Adjust if actual table size differs significantly.';
  }

  if (rows > 100_000_000) {
    numParts = Math.max(numParts, 50000);
    numPartsComment += ' Increased to ≥50,000 for tables with >100M rows to improve parallelism.';
  }

  props['spark.cdm.perfops.numParts'] = numParts;
  comments['spark.cdm.perfops.numParts'] = numPartsComment;

  // ── batchSize ─────────────────────────────────────────────────────────────
  let batchSize = 5;
  let batchSizeComment = 'Default batch size for writing to target.';

  if (isPartitionKeyOnly) {
    batchSize = 1;
    batchSizeComment =
      'Set to 1 because primary key = partition key (no clustering columns). ' +
      'Each row is its own partition, so batching across rows would span multiple partitions.';
  } else if (hasLOBs) {
    batchSize = 1;
    batchSizeComment =
      'Set to 1 because LOB (BLOB/large TEXT) columns are present. ' +
      'Average row size likely exceeds 20KB, making multi-row batches inefficient.';
  } else if (sizeGB > 0 && rows > 0) {
    const avgRowKB = (sizeGB * 1024 * 1024) / rows;
    if (avgRowKB > 20) {
      batchSize = 1;
      batchSizeComment =
        `Set to 1 because estimated average row size (~${avgRowKB.toFixed(1)} KB) exceeds 20KB. ` +
        'Large rows should not be batched together.';
    } else if (avgRowKB < 1 && !isPartitionKeyOnly) {
      batchSize = 20;
      batchSizeComment =
        `Increased to 20 because estimated average row size (~${avgRowKB.toFixed(2)} KB) is very small. ` +
        'Small rows benefit from larger batches to reduce write overhead.';
    }
  }

  props['spark.cdm.perfops.batchSize'] = batchSize;
  comments['spark.cdm.perfops.batchSize'] = batchSizeComment;

  // ── fetchSizeInRows ───────────────────────────────────────────────────────
  let fetchSize = 1000;
  let fetchSizeComment = 'Default fetch size (1000 rows per read from origin).';

  if (hasLOBs) {
    fetchSize = 100;
    fetchSizeComment =
      'Reduced to 100 because LOB columns are present. ' +
      'Large fields increase memory pressure; smaller fetch size prevents OOM errors.';
  } else if (sizeGB > 0 && rows > 0) {
    const avgRowKB = (sizeGB * 1024 * 1024) / rows;
    if (avgRowKB > 100) {
      fetchSize = 200;
      fetchSizeComment =
        `Reduced to 200 because estimated average row size (~${avgRowKB.toFixed(0)} KB) is large. ` +
        'Prevents excessive memory usage during reads.';
    }
  }

  if (fetchSize !== 1000) {
    props['spark.cdm.perfops.fetchSizeInRows'] = fetchSize;
    comments['spark.cdm.perfops.fetchSizeInRows'] = fetchSizeComment;
  }

  // ── ratelimit ─────────────────────────────────────────────────────────────
  let rateLimit = 20000;
  let rateLimitComment =
    'Default rate limit (20,000 ops/sec per CDM VM). ' +
    'Increase after validating origin/target cluster capacity.';

  if (hasLOBs) {
    rateLimit = 5000;
    rateLimitComment =
      'Reduced to 5,000 ops/sec due to LOB columns. ' +
      'Large field reads/writes are slower; higher rates risk timeouts.';
  } else if (rows > 500_000_000 || sizeGB > 500) {
    rateLimit = 40000;
    rateLimitComment =
      'Increased to 40,000 ops/sec for large table. ' +
      'Monitor cluster load and reduce if you see timeouts or errors.';
  }

  props['spark.cdm.perfops.ratelimit.origin'] = rateLimit;
  comments['spark.cdm.perfops.ratelimit.origin'] = rateLimitComment;
  props['spark.cdm.perfops.ratelimit.target'] = rateLimit;
  comments['spark.cdm.perfops.ratelimit.target'] =
    rateLimitComment +
    ' Set equal to origin rate limit; increase if using ExplodeMap (more target writes).';

  // ── TTL/Writetime with collections ────────────────────────────────────────
  const onlyCollectionNonPK = hasCollections && !hasNonCollectionNonPKColumns(originSchema);

  if (onlyCollectionNonPK || hasUDTs) {
    props['spark.cdm.schema.ttlwritetime.calc.useCollections'] = true;
    comments['spark.cdm.schema.ttlwritetime.calc.useCollections'] =
      'Enabled because non-key columns are collections/UDTs. ' +
      'Without this, TTL and WRITETIME cannot be determined from non-key columns, ' +
      'causing target rows to have no TTL or incorrect WRITETIME.';
  }

  // ── Counter tables ────────────────────────────────────────────────────────
  if (hasCounters) {
    props['spark.cdm.autocorrect.missing.counter'] = false;
    comments['spark.cdm.autocorrect.missing.counter'] =
      'Counter tables require special care. Left false by default. ' +
      'Re-inserting a deleted counter row can cause double-counting (e.g., 5323 → 10646). ' +
      'Only enable after carefully reviewing counter semantics.';
  }

  // ── Timestamp codec recommendation ───────────────────────────────────────
  if (hasTimestamps) {
    comments['spark.cdm.transform.codecs'] =
      'If timestamps are stored as TEXT (epoch millis), enable TIMESTAMP_STRING_MILLIS codec. ' +
      'If stored as formatted strings, enable TIMESTAMP_STRING_FORMAT and set .timestamp.string.format.';
  }

  // ── Numeric codec recommendation ─────────────────────────────────────────
  if (hasNumerics) {
    comments['spark.cdm.transform.codecs_numerics'] =
      'If numeric types (INT, DOUBLE, BIGINT, DECIMAL) are stored as TEXT in origin or target, ' +
      'enable the appropriate codec: INT_STRING, DOUBLE_STRING, BIGINT_STRING, or DECIMAL_STRING.';
  }

  // ── Large table Spark cluster recommendation ──────────────────────────────
  if (sizeGB > 1000 || rows > 1_000_000_000) {
    comments['spark_cluster_recommendation'] =
      'RECOMMENDATION: Table is very large (>1TB or >1B rows). ' +
      'Consider using a Spark Cluster (Databricks, Google Dataproc) instead of a single VM ' +
      'for better throughput and fault tolerance.';
  }

  // ── TrackRun recommendation for large tables ──────────────────────────────
  if (sizeGB > 100 || rows > 100_000_000) {
    comments['spark.cdm.trackRun'] =
      'Recommended: enable trackRun for large tables so migration can be resumed ' +
      'if the job is interrupted. Set spark.cdm.trackRun=true and ' +
      'spark.cdm.trackRun.autoRerun=true for automatic resume.';
  }

  return { props, comments };
}

/**
 * Returns true if the schema has at least one non-PK, non-collection column.
 */
function hasNonCollectionNonPKColumns(schema) {
  if (!schema) return true; // assume yes if unknown
  const pkSet = new Set(schema.allPrimaryKeys || []);
  return schema.columns.some((col) => {
    if (pkSet.has(col.name)) return false;
    const t = col.type.toLowerCase();
    return (
      !t.includes('list<') && !t.includes('set<') && !t.includes('map<') && !t.includes('frozen<')
    );
  });
}
