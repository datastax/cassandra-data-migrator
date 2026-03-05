import { describe, it, expect } from 'vitest';
import { applyBestPractices } from '../utils/bestPracticesRules.js';

// Helper: minimal schema with no special types
const baseSchema = (overrides = {}) => ({
  keyspace: 'ks',
  table: 'tbl',
  keyspaceTable: 'ks.tbl',
  columns: [
    { name: 'id', type: 'uuid' },
    { name: 'name', type: 'text' },
  ],
  partitionKeys: ['id'],
  clusteringKeys: ['name'],
  allPrimaryKeys: ['id', 'name'],
  isPartitionKeyOnly: false,
  hasCollections: false,
  hasUDTs: false,
  hasCounters: false,
  hasBlobs: false,
  hasTimestamps: false,
  hasNumerics: false,
  hasFrozen: false,
  ...overrides,
});

describe('applyBestPractices', () => {
  // ── Default outputs ───────────────────────────────────────────────────────
  it('returns props and comments objects', () => {
    const { props, comments } = applyBestPractices({});
    expect(props).toBeDefined();
    expect(comments).toBeDefined();
  });

  it('always sets numParts, batchSize, ratelimit.origin, ratelimit.target', () => {
    const { props } = applyBestPractices({});
    expect(props['spark.cdm.perfops.numParts']).toBeDefined();
    expect(props['spark.cdm.perfops.batchSize']).toBeDefined();
    expect(props['spark.cdm.perfops.ratelimit.origin']).toBeDefined();
    expect(props['spark.cdm.perfops.ratelimit.target']).toBeDefined();
  });

  it('defaults: numParts=5000, batchSize=5, rateLimit=20000', () => {
    const { props } = applyBestPractices({});
    expect(props['spark.cdm.perfops.numParts']).toBe(5000);
    expect(props['spark.cdm.perfops.batchSize']).toBe(5);
    expect(props['spark.cdm.perfops.ratelimit.origin']).toBe(20000);
    expect(props['spark.cdm.perfops.ratelimit.target']).toBe(20000);
  });

  // ── numParts from tableSizeGB ─────────────────────────────────────────────
  it('calculates numParts from tableSizeGB', () => {
    const { props } = applyBestPractices({ tableSizeGB: 100 });
    // ceil(100 * 1024 / 10) = 10240
    expect(props['spark.cdm.perfops.numParts']).toBe(10240);
  });

  it('enforces minimum numParts of 1000 for small tables', () => {
    const { props } = applyBestPractices({ tableSizeGB: 0.05 });
    expect(props['spark.cdm.perfops.numParts']).toBeGreaterThanOrEqual(1000);
  });

  it('calculates numParts from rowCount when no tableSizeGB', () => {
    const { props } = applyBestPractices({ rowCount: 10_000_000 });
    // 10M rows ≈ 10GB → ceil(10*1024/10) = 1024
    expect(props['spark.cdm.perfops.numParts']).toBe(1024);
  });

  it('enforces numParts >= 50000 for >100M rows', () => {
    const { props } = applyBestPractices({ rowCount: 200_000_000 });
    expect(props['spark.cdm.perfops.numParts']).toBeGreaterThanOrEqual(50000);
  });

  // ── batchSize rules ───────────────────────────────────────────────────────
  it('sets batchSize=1 when isPartitionKeyOnly', () => {
    const schema = baseSchema({ isPartitionKeyOnly: true, clusteringKeys: [] });
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.perfops.batchSize']).toBe(1);
  });

  it('sets batchSize=1 when hasBlobs (LOBs)', () => {
    const schema = baseSchema({ hasBlobs: true });
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.perfops.batchSize']).toBe(1);
  });

  it('sets batchSize=1 when dataTypes includes lobs', () => {
    const { props } = applyBestPractices({ dataTypes: ['lobs'] });
    expect(props['spark.cdm.perfops.batchSize']).toBe(1);
  });

  it('sets batchSize=1 when avg row > 20KB', () => {
    // 1GB / 1000 rows = 1MB per row >> 20KB
    const { props } = applyBestPractices({ tableSizeGB: 1, rowCount: 1000 });
    expect(props['spark.cdm.perfops.batchSize']).toBe(1);
  });

  it('sets batchSize=20 for very small rows', () => {
    // 1GB / 10M rows = 0.1KB per row < 1KB, not partition-key-only
    const schema = baseSchema({ isPartitionKeyOnly: false });
    const { props } = applyBestPractices({
      originSchema: schema,
      tableSizeGB: 1,
      rowCount: 10_000_000,
    });
    expect(props['spark.cdm.perfops.batchSize']).toBe(20);
  });

  // ── fetchSizeInRows ───────────────────────────────────────────────────────
  it('does not set fetchSizeInRows by default', () => {
    const { props } = applyBestPractices({});
    expect(props['spark.cdm.perfops.fetchSizeInRows']).toBeUndefined();
  });

  it('sets fetchSizeInRows=100 when LOBs present', () => {
    const { props } = applyBestPractices({ dataTypes: ['lobs'] });
    expect(props['spark.cdm.perfops.fetchSizeInRows']).toBe(100);
  });

  it('sets fetchSizeInRows=200 when avg row > 100KB', () => {
    // 10GB / 10000 rows = 1MB per row
    const { props } = applyBestPractices({ tableSizeGB: 10, rowCount: 10000 });
    expect(props['spark.cdm.perfops.fetchSizeInRows']).toBe(200);
  });

  // ── Rate limit ────────────────────────────────────────────────────────────
  it('reduces rateLimit to 5000 for LOBs', () => {
    const { props } = applyBestPractices({ dataTypes: ['lobs'] });
    expect(props['spark.cdm.perfops.ratelimit.origin']).toBe(5000);
  });

  it('increases rateLimit to 40000 for large tables (>500M rows)', () => {
    const { props } = applyBestPractices({ rowCount: 600_000_000 });
    expect(props['spark.cdm.perfops.ratelimit.origin']).toBe(40000);
  });

  it('increases rateLimit to 40000 for large tables (>500GB)', () => {
    const { props } = applyBestPractices({ tableSizeGB: 600 });
    expect(props['spark.cdm.perfops.ratelimit.origin']).toBe(40000);
  });

  // ── TTL/Writetime with collections ────────────────────────────────────────
  it('enables useCollections when all non-PK columns are collections', () => {
    const schema = {
      ...baseSchema(),
      columns: [
        { name: 'id', type: 'uuid' },
        { name: 'tags', type: 'set<text>' },
      ],
      partitionKeys: ['id'],
      clusteringKeys: [],
      allPrimaryKeys: ['id'],
      hasCollections: true,
    };
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.schema.ttlwritetime.calc.useCollections']).toBe(true);
  });

  it('enables useCollections when hasUDTs', () => {
    const schema = baseSchema({ hasUDTs: true });
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.schema.ttlwritetime.calc.useCollections']).toBe(true);
  });

  it('does not enable useCollections when non-PK non-collection columns exist', () => {
    // Schema with a non-PK, non-collection column ('description text')
    const schema = {
      ...baseSchema(),
      columns: [
        { name: 'id', type: 'uuid' },
        { name: 'tags', type: 'set<text>' },
        { name: 'description', type: 'text' }, // non-PK, non-collection
      ],
      partitionKeys: ['id'],
      clusteringKeys: [],
      allPrimaryKeys: ['id'],
      hasCollections: true,
    };
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.schema.ttlwritetime.calc.useCollections']).toBeUndefined();
  });

  // ── Counter tables ────────────────────────────────────────────────────────
  it('sets autocorrect.missing.counter=false for counter tables', () => {
    const schema = baseSchema({ hasCounters: true });
    const { props } = applyBestPractices({ originSchema: schema });
    expect(props['spark.cdm.autocorrect.missing.counter']).toBe(false);
  });

  it('sets autocorrect.missing.counter=false when dataTypes includes counters', () => {
    const { props } = applyBestPractices({ dataTypes: ['counters'] });
    expect(props['spark.cdm.autocorrect.missing.counter']).toBe(false);
  });

  it('does not set autocorrect.missing.counter for non-counter tables', () => {
    const { props } = applyBestPractices({});
    expect(props['spark.cdm.autocorrect.missing.counter']).toBeUndefined();
  });

  // ── Comments ──────────────────────────────────────────────────────────────
  it('adds timestamp codec comment when hasTimestamps', () => {
    const schema = baseSchema({ hasTimestamps: true });
    const { comments } = applyBestPractices({ originSchema: schema });
    expect(comments['spark.cdm.transform.codecs']).toContain('TIMESTAMP_STRING_MILLIS');
  });

  it('adds numeric codec comment when hasNumerics', () => {
    const schema = baseSchema({ hasNumerics: true });
    const { comments } = applyBestPractices({ originSchema: schema });
    expect(comments['spark.cdm.transform.codecs_numerics']).toContain('INT_STRING');
  });

  it('adds spark cluster recommendation for >1TB tables', () => {
    const { comments } = applyBestPractices({ tableSizeGB: 1500 });
    expect(comments['spark_cluster_recommendation']).toContain('RECOMMENDATION');
  });

  it('adds spark cluster recommendation for >1B rows', () => {
    const { comments } = applyBestPractices({ rowCount: 2_000_000_000 });
    expect(comments['spark_cluster_recommendation']).toContain('RECOMMENDATION');
  });

  it('does not add spark cluster recommendation for small tables', () => {
    const { comments } = applyBestPractices({ tableSizeGB: 50 });
    expect(comments['spark_cluster_recommendation']).toBeUndefined();
  });

  it('adds trackRun recommendation for large tables (>100GB)', () => {
    const { comments } = applyBestPractices({ tableSizeGB: 200 });
    expect(comments['spark.cdm.trackRun']).toContain('trackRun');
  });

  it('adds trackRun recommendation for >100M rows', () => {
    const { comments } = applyBestPractices({ rowCount: 150_000_000 });
    expect(comments['spark.cdm.trackRun']).toContain('trackRun');
  });

  it('does not add trackRun recommendation for small tables', () => {
    const { comments } = applyBestPractices({ tableSizeGB: 10 });
    expect(comments['spark.cdm.trackRun']).toBeUndefined();
  });
});
