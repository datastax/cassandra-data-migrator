import { describe, it, expect } from 'vitest';
import { parseCqlSchema } from '../utils/parseCqlSchema.js';

describe('parseCqlSchema', () => {
  // ── Null / empty input ────────────────────────────────────────────────────
  it('returns null for null input', () => {
    expect(parseCqlSchema(null)).toBeNull();
  });

  it('returns null for empty string', () => {
    expect(parseCqlSchema('')).toBeNull();
  });

  it('returns null for whitespace-only string', () => {
    expect(parseCqlSchema('   ')).toBeNull();
  });

  // ── Basic table parsing ───────────────────────────────────────────────────
  it('parses simple table with single partition key', () => {
    const cql = `
      CREATE TABLE ks.users (
        id uuid PRIMARY KEY,
        name text,
        age int
      );
    `;
    const result = parseCqlSchema(cql);
    expect(result.keyspace).toBe('ks');
    expect(result.table).toBe('users');
    expect(result.keyspaceTable).toBe('ks.users');
    expect(result.partitionKeys).toEqual(['id']);
    expect(result.clusteringKeys).toEqual([]);
    expect(result.isPartitionKeyOnly).toBe(true);
    expect(result.parseError).toBeNull();
  });

  it('parses table with composite partition key and clustering key', () => {
    const cql = `
      CREATE TABLE myks.events (
        tenant_id text,
        event_date date,
        event_id uuid,
        payload text,
        PRIMARY KEY ((tenant_id, event_date), event_id)
      );
    `;
    const result = parseCqlSchema(cql);
    expect(result.keyspace).toBe('myks');
    expect(result.table).toBe('events');
    expect(result.partitionKeys).toEqual(['tenant_id', 'event_date']);
    expect(result.clusteringKeys).toEqual(['event_id']);
    expect(result.allPrimaryKeys).toEqual(['tenant_id', 'event_date', 'event_id']);
    expect(result.isPartitionKeyOnly).toBe(false);
  });

  it('parses table with single partition key and multiple clustering keys', () => {
    const cql = `
      CREATE TABLE ks.sensor_data (
        sensor_id text,
        year int,
        month int,
        value double,
        PRIMARY KEY (sensor_id, year, month)
      );
    `;
    const result = parseCqlSchema(cql);
    expect(result.partitionKeys).toEqual(['sensor_id']);
    expect(result.clusteringKeys).toEqual(['year', 'month']);
    expect(result.isPartitionKeyOnly).toBe(false);
  });

  it('parses IF NOT EXISTS syntax', () => {
    const cql = `
      CREATE TABLE IF NOT EXISTS ks.tbl (
        id uuid PRIMARY KEY,
        val text
      );
    `;
    const result = parseCqlSchema(cql);
    expect(result.keyspace).toBe('ks');
    expect(result.table).toBe('tbl');
    expect(result.parseError).toBeNull();
  });

  // ── Column detection ──────────────────────────────────────────────────────
  it('detects all columns', () => {
    const cql = `
      CREATE TABLE ks.tbl (
        id uuid PRIMARY KEY,
        name text,
        age int
      );
    `;
    const result = parseCqlSchema(cql);
    const names = result.columns.map((c) => c.name);
    expect(names).toContain('id');
    expect(names).toContain('name');
    expect(names).toContain('age');
  });

  // ── Special type detection ────────────────────────────────────────────────
  it('detects blob columns', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, data blob);`;
    expect(parseCqlSchema(cql).hasBlobs).toBe(true);
  });

  it('detects counter columns', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, hits counter);`;
    expect(parseCqlSchema(cql).hasCounters).toBe(true);
  });

  it('detects timestamp columns', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, created_at timestamp);`;
    expect(parseCqlSchema(cql).hasTimestamps).toBe(true);
  });

  it('detects date columns as timestamps', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, d date);`;
    expect(parseCqlSchema(cql).hasTimestamps).toBe(true);
  });

  it('detects numeric columns (double)', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, score double);`;
    expect(parseCqlSchema(cql).hasNumerics).toBe(true);
  });

  it('detects numeric columns (bigint)', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, n bigint);`;
    expect(parseCqlSchema(cql).hasNumerics).toBe(true);
  });

  it('detects list collections', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, tags list<text>);`;
    const result = parseCqlSchema(cql);
    expect(result.hasCollections).toBe(true);
  });

  it('detects set collections', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, tags set<text>);`;
    expect(parseCqlSchema(cql).hasCollections).toBe(true);
  });

  it('detects map collections', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, props map<text, text>);`;
    expect(parseCqlSchema(cql).hasCollections).toBe(true);
  });

  it('detects frozen types', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, data frozen<list<text>>);`;
    expect(parseCqlSchema(cql).hasFrozen).toBe(true);
  });

  it('detects UDTs (non-builtin types)', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, addr my_address_type);`;
    expect(parseCqlSchema(cql).hasUDTs).toBe(true);
  });

  it('does not flag built-in types as UDTs', () => {
    const cql = `CREATE TABLE ks.t (id uuid PRIMARY KEY, name text, age int, score double);`;
    const result = parseCqlSchema(cql);
    // uuid, text, int, double are all built-in
    expect(result.hasUDTs).toBe(false);
  });

  // ── Error cases ───────────────────────────────────────────────────────────
  it('sets parseError when no keyspace.table pattern found', () => {
    const cql = `CREATE TABLE users (id uuid PRIMARY KEY);`;
    const result = parseCqlSchema(cql);
    expect(result.parseError).toBeTruthy();
  });

  it('returns result object (not null) even on parse error', () => {
    const result = parseCqlSchema('not a valid cql statement');
    expect(result).not.toBeNull();
    expect(result.parseError).toBeTruthy();
  });

  // ── Complex real-world example ────────────────────────────────────────────
  it('parses a realistic IoT table', () => {
    const cql = `
      CREATE TABLE IF NOT EXISTS iot.sensor_readings (
        device_id text,
        bucket_date date,
        reading_time timeuuid,
        temperature double,
        humidity float,
        metadata map<text, text>,
        tags set<text>,
        PRIMARY KEY ((device_id, bucket_date), reading_time)
      ) WITH CLUSTERING ORDER BY (reading_time DESC);
    `;
    const result = parseCqlSchema(cql);
    expect(result.keyspace).toBe('iot');
    expect(result.table).toBe('sensor_readings');
    expect(result.partitionKeys).toEqual(['device_id', 'bucket_date']);
    expect(result.clusteringKeys).toEqual(['reading_time']);
    expect(result.hasCollections).toBe(true);
    expect(result.hasNumerics).toBe(true);
    expect(result.hasTimestamps).toBe(true);
    expect(result.isPartitionKeyOnly).toBe(false);
    expect(result.parseError).toBeNull();
  });
});
