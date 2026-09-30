// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { makeRuntimeSqliteLayer, makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import { PurgeDatabaseInUseError, purgeDeletedThreads } from "./purge.ts";

const CUTOFF = "2026-09-01T00:00:00.000Z";

const tempDatabase = () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-purge-"));
  return NodePath.join(dir, "state.sqlite");
};

/** Three threads: deleted before the cutoff, deleted after it, and live. */
const seed = (dbPath: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threads = [
      ["old", "2026-08-01T00:00:00.000Z"],
      ["recent", "2026-09-15T00:00:00.000Z"],
      ["live", null],
    ] as const;
    for (const [id, deletedAt] of threads) {
      yield* sql`
        INSERT INTO projection_threads (thread_id, project_id, title, created_at, updated_at, deleted_at)
        VALUES (${id}, 'project', ${id}, '2026-07-01T00:00:00.000Z', '2026-07-01T00:00:00.000Z', ${deletedAt})
      `;
      yield* sql`
        INSERT INTO projection_thread_sessions (thread_id, status, updated_at)
        VALUES (${id}, 'stopped', '2026-07-01T00:00:00.000Z')
      `;
      for (const version of [1, 2]) {
        yield* sql`
          INSERT INTO orchestration_events
            (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
             command_id, actor_kind, payload_json, metadata_json)
          VALUES (${`${id}-${version}`}, 'thread', ${id}, ${version}, 'thread.message-sent',
                  '2026-07-01T00:00:00.000Z', ${`${id}-command-${version}`}, 'client', '{}', '{}')
        `;
        yield* sql`
          INSERT INTO orchestration_command_receipts
            (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
          VALUES (${`${id}-command-${version}`}, 'thread', ${id}, '2026-07-01T00:00:00.000Z', 0, 'accepted')
        `;
      }
    }
  }).pipe(
    Effect.provide(makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer))),
  );

const remaining = (dbPath: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const count = (rows: ReadonlyArray<{ readonly n: number }>) => rows[0]?.n ?? 0;
    return {
      threads: (yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM projection_threads ORDER BY thread_id
      `).map((row) => row.thread_id),
      events: count(
        yield* sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM orchestration_events`,
      ),
      receipts: count(
        yield* sql<{
          readonly n: number;
        }>`SELECT COUNT(*) AS n FROM orchestration_command_receipts`,
      ),
      sessions: count(
        yield* sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM projection_thread_sessions`,
      ),
    };
  }).pipe(Effect.provide(makeRuntimeSqliteLayer({ filename: dbPath })));

const purge = (dbPath: string, dryRun = false) =>
  purgeDeletedThreads({ dbPath, cutoff: CUTOFF, dryRun }).pipe(
    Effect.provide(makeRuntimeSqliteLayer({ filename: dbPath })),
  );

it.effect("removes only threads deleted before the cutoff, with their events and receipts", () =>
  Effect.gen(function* () {
    const dbPath = tempDatabase();
    yield* seed(dbPath);
    const result = yield* purge(dbPath);
    assert.deepStrictEqual(result, { threads: 1, events: 2, receipts: 2, projectionRows: 2 });
    assert.deepStrictEqual(yield* remaining(dbPath), {
      threads: ["live", "recent"],
      events: 4,
      receipts: 4,
      sessions: 2,
    });
  }),
);

it.effect("counts without changing anything in a dry run", () =>
  Effect.gen(function* () {
    const dbPath = tempDatabase();
    yield* seed(dbPath);
    const result = yield* purge(dbPath, true);
    assert.strictEqual(result.threads, 1);
    assert.strictEqual(result.events, 2);
    assert.deepStrictEqual((yield* remaining(dbPath)).threads, ["live", "old", "recent"]);
  }),
);

const holderSource = `
const { DatabaseSync } = require("node:sqlite");
const db = new DatabaseSync(process.argv[1]);
db.prepare("SELECT COUNT(*) FROM projection_threads").get();
process.stdout.write("open\\n");
setInterval(() => {}, 1000);
`;

it.effect("refuses to purge while another process has the database open", () =>
  Effect.gen(function* () {
    const dbPath = tempDatabase();
    yield* seed(dbPath);
    // An idle connection, as a running server holds between requests.
    const holder = yield* Effect.acquireRelease(
      Effect.promise(
        () =>
          new Promise<NodeChildProcess.ChildProcess>((resolve, reject) => {
            const child = NodeChildProcess.spawn(process.execPath, ["-e", holderSource, dbPath], {
              stdio: ["ignore", "pipe", "ignore"],
            });
            child.stdout.once("data", () => resolve(child));
            child.on("error", reject);
          }),
      ),
      (child) => Effect.sync(() => child.kill()),
    );
    assert.isDefined(holder.pid);
    const error = yield* Effect.flip(purge(dbPath));
    assert.instanceOf(error, PurgeDatabaseInUseError);
    assert.deepStrictEqual((yield* remaining(dbPath)).threads, ["live", "old", "recent"]);
  }).pipe(Effect.scoped),
);
