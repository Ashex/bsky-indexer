# bsky-indexer

A service to ingest data from an atproto relay into the Bluesky AppView.

## Environment Variables

### Required

- `BSKY_DB_POSTGRES_URL`: The URL of the Postgres database to connect to.
- `BSKY_DB_POSTGRES_SCHEMA`: The Postgres database schema to connect to (usually `bsky`).
- `BSKY_REPO_PROVIDER`: The URL of the relay to connect to (`wss://hostname`).
- `BSKY_DID_PLC_URL`/`DID_PLC_URL`: The URL of the PLC directory to use (usually
  `https://did.plc.directory`).

### Optional

- `REDIS_URL`: Connection URL of a Redis instance to cache the relay cursor.
- `BSKY_DB_POOL_SIZE`: The size of the Postgres connection pool to use (default: 200).
- `SUB_MIN_WORKERS`: The minimum number of workers to process events (default based on CPU count,
  between 16 and 32).
- `SUB_MAX_WORKERS`: The maximum number of workers to process events (default based on CPU count,
  between 32 and 64).
- `SUB_MAX_WORKER_CONCURRENCY`: The maximum number of concurrent events to process per worker
  (default: 75).
- `SUB_MAX_TIMESTAMP_DELTA_MS`: The maximum time delta between an event's stated time and the
  observed indexing time to accept. (default: 1000 * 60 * 10 = 10 minutes).
- `STATS_FREQUENCY_MS`: The frequency at which to log stats (default: 30_000 ms).
