-- One SQLite database file per attempt-segment (docker-executor.md §8.4).
--
-- A segment holds one time-contiguous slice of a single attempt's telemetry;
-- the enclosing directory names (`<job-id>/<attempt-id>/`) carry the identity,
-- so only the `allocation_id` — which can differ per sample across a retry that
-- reuses an attempt id boundary — is stored per row. Retention deletes whole
-- segment files, so nothing here ever runs `DELETE`.

CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);

CREATE TABLE metrics (
  id INTEGER PRIMARY KEY,          -- rowid: insertion order
  at INTEGER NOT NULL,             -- µs since epoch
  allocation_id TEXT NOT NULL,
  cpu_usage_total_us INTEGER NOT NULL,
  cpu_throttled_total_us INTEGER NOT NULL,
  memory_used_bytes INTEGER NOT NULL,
  memory_peak_bytes INTEGER NOT NULL,
  disk_writable_bytes INTEGER NOT NULL,
  disk_image_bytes INTEGER NOT NULL,
  net_rx_bytes_total INTEGER NOT NULL,
  net_tx_bytes_total INTEGER NOT NULL,
  blkio_read_bytes_total INTEGER NOT NULL,
  blkio_write_bytes_total INTEGER NOT NULL
);
CREATE INDEX metrics_at ON metrics(at);

CREATE TABLE log_chunks (
  id INTEGER PRIMARY KEY,
  at INTEGER NOT NULL,
  allocation_id TEXT NOT NULL,
  stream INTEGER NOT NULL,         -- 0=stdout 1=stderr
  bytes BLOB NOT NULL
);
CREATE INDEX log_chunks_at ON log_chunks(at);
