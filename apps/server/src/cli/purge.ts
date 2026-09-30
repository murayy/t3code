import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/unstable/cli";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import { resolveBaseDir } from "../os-jank.ts";
import { makeRuntimeSqliteLayer } from "../persistence/Layers/Sqlite.ts";
import { isProcessAlive, readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import { baseDirFlag } from "./config.ts";

/** Read-model tables keyed by `thread_id`; `projection_threads` goes last. */
const THREAD_TABLES = [
  "projection_thread_activities",
  "projection_thread_messages",
  "projection_thread_proposed_plans",
  "projection_thread_pull_requests",
  "projection_thread_sessions",
  "projection_pending_approvals",
  "projection_turns",
  "provider_session_runtime",
  "checkpoint_diff_blobs",
  "projection_threads",
] as const;

export class PurgeServerRunningError extends Schema.TaggedErrorClass<PurgeServerRunningError>()(
  "PurgeServerRunningError",
  { pid: Schema.Int, origin: Schema.String },
) {
  override get message(): string {
    return `A T3 Code server (pid ${String(this.pid)}, ${this.origin}) is running on this data directory. Stop it before purging.`;
  }
}

export class PurgeDatabaseInUseError extends Schema.TaggedErrorClass<PurgeDatabaseInUseError>()(
  "PurgeDatabaseInUseError",
  { dbPath: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `${this.dbPath} is open in another process. Stop every T3 Code server and client using it before purging.`;
  }
}

export class PurgeDatabaseMissingError extends Schema.TaggedErrorClass<PurgeDatabaseMissingError>()(
  "PurgeDatabaseMissingError",
  { dbPath: Schema.String },
) {
  override get message(): string {
    return `No T3 Code database at ${this.dbPath}.`;
  }
}

export interface PurgeResult {
  readonly threads: number;
  readonly events: number;
  readonly receipts: number;
  readonly projectionRows: number;
}

const changes = (sql: SqlClient.SqlClient) =>
  sql<{ readonly n: number }>`SELECT changes() AS n`.pipe(Effect.map((rows) => rows[0]?.n ?? 0));

/**
 * Removes the events, command receipts and read-model rows of threads deleted
 * before `cutoff` (an ISO timestamp), then compacts the database.
 *
 * The connection takes SQLite's exclusive lock first and holds it throughout,
 * so the purge fails instead of waiting when any other process has the
 * database open, and no process can open it until the purge finishes.
 * A dry run counts the same rows inside the transaction and rolls it back.
 */
export const purgeDeletedThreads = Effect.fn("purgeDeletedThreads")(function* (input: {
  readonly dbPath: string;
  readonly cutoff: string;
  readonly dryRun: boolean;
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA busy_timeout = 0`;
  yield* sql`PRAGMA locking_mode = EXCLUSIVE`;
  yield* sql`BEGIN EXCLUSIVE`.pipe(
    Effect.mapError((cause) => new PurgeDatabaseInUseError({ dbPath: input.dbPath, cause })),
  );

  const purge = Effect.gen(function* () {
    const tables = new Set(
      (yield* sql<{
        readonly name: string;
      }>`SELECT name FROM sqlite_master WHERE type = 'table'`).map((row) => row.name),
    );
    yield* sql`CREATE TEMP TABLE purge_threads(thread_id TEXT PRIMARY KEY)`;
    yield* sql`
      INSERT INTO purge_threads
      SELECT thread_id FROM projection_threads
      WHERE deleted_at IS NOT NULL AND deleted_at < ${input.cutoff}
    `;
    const threads =
      (yield* sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM purge_threads`)[0]?.n ?? 0;

    yield* sql`
      DELETE FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id IN (SELECT thread_id FROM purge_threads)
    `;
    const events = yield* changes(sql);
    yield* sql`
      DELETE FROM orchestration_command_receipts
      WHERE aggregate_kind = 'thread' AND aggregate_id IN (SELECT thread_id FROM purge_threads)
    `;
    const receipts = yield* changes(sql);
    let projectionRows = 0;
    for (const table of THREAD_TABLES) {
      if (!tables.has(table)) continue;
      yield* sql.unsafe(
        `DELETE FROM ${table} WHERE thread_id IN (SELECT thread_id FROM purge_threads)`,
      );
      projectionRows += yield* changes(sql);
    }
    yield* sql`DROP TABLE purge_threads`;
    return { threads, events, receipts, projectionRows } satisfies PurgeResult;
  });

  const result = yield* purge.pipe(Effect.onError(() => sql`ROLLBACK`.pipe(Effect.ignore)));
  if (input.dryRun) {
    yield* sql`ROLLBACK`;
    return result;
  }
  yield* sql`COMMIT`;
  if (result.threads > 0) {
    yield* sql`VACUUM`;
    yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
  }
  return result;
});

const databaseBytes = (dbPath: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    let total = 0;
    for (const file of [dbPath, `${dbPath}-wal`]) {
      const info = yield* fs.stat(file).pipe(Effect.option);
      if (Option.isSome(info)) total += Number(info.value.size);
    }
    return total;
  });

const formatMiB = (bytes: number) => `${(bytes / 1024 / 1024).toFixed(1)} MiB`;

const runPurgeCommand = Effect.fn("cli.purge")(function* (flags: {
  readonly baseDir: Option.Option<string>;
  readonly deletedOlderThan: number;
  readonly dryRun: boolean;
}) {
  const t3Home = yield* Config.string("T3CODE_HOME").pipe(Config.option);
  const baseDir = yield* resolveBaseDir(
    Option.getOrUndefined(flags.baseDir) ?? Option.getOrUndefined(t3Home),
  );
  const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined);

  const runtime = yield* readPersistedServerRuntimeState(paths.serverRuntimeStatePath).pipe(
    Effect.orElseSucceed(() => Option.none()),
  );
  if (Option.isSome(runtime) && isProcessAlive(runtime.value.pid)) {
    return yield* new PurgeServerRunningError({
      pid: runtime.value.pid,
      origin: runtime.value.origin,
    });
  }
  const fs = yield* FileSystem.FileSystem;
  if (!(yield* fs.exists(paths.dbPath))) {
    return yield* new PurgeDatabaseMissingError({ dbPath: paths.dbPath });
  }

  const cutoff = DateTime.formatIso(
    DateTime.subtractDuration(yield* DateTime.now, Duration.days(flags.deletedOlderThan)),
  );
  const before = yield* databaseBytes(paths.dbPath);
  const result = yield* purgeDeletedThreads({
    dbPath: paths.dbPath,
    cutoff,
    dryRun: flags.dryRun,
  }).pipe(Effect.provide(makeRuntimeSqliteLayer({ filename: paths.dbPath })));
  const after = yield* databaseBytes(paths.dbPath);

  const verb = flags.dryRun ? "Would purge" : "Purged";
  yield* Console.log(
    `${verb} ${String(result.threads)} threads deleted before ${cutoff}: ` +
      `${String(result.events)} events, ${String(result.receipts)} command receipts, ` +
      `${String(result.projectionRows)} read-model rows.`,
  );
  if (!flags.dryRun) {
    yield* Console.log(`${paths.dbPath}: ${formatMiB(before)} -> ${formatMiB(after)}`);
  }
});

export const purgeCommand = Command.make("purge", {
  baseDir: baseDirFlag,
  deletedOlderThan: Flag.integer("deleted-older-than").pipe(
    Flag.withSchema(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
    Flag.withDescription("Purge threads deleted at least this many days ago."),
  ),
  dryRun: Flag.boolean("dry-run").pipe(
    Flag.withDescription("Count what would be purged and change nothing."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Remove deleted threads from a stopped server's database, then compact it.",
  ),
  Command.withHandler(runPurgeCommand),
);
