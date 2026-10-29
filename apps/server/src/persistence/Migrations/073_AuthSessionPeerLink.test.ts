import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";
import migrateAuthSessionPeerLink from "./073_AuthSessionPeerLink.ts";

const layer = it.layer(NodeSqliteClient.layer({ filename: ":memory:" }));

layer("073_AuthSessionPeerLink", (it) => {
  it.effect("backfills legacy linked sessions and leaves unrelated sessions unchanged", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 72 });
      const insert = (sessionId: string, subject: string, label: string, deviceType = "bot") =>
        sql`
          INSERT INTO auth_sessions (
            session_id, subject, scopes, method, client_label, client_device_type,
            issued_at, expires_at, revoked_at
          ) VALUES (
            ${sessionId}, ${subject}, '[]', 'bearer-access-token', ${label}, ${deviceType},
            '2026-10-29T00:00:00.000Z', '2026-10-30T00:00:00.000Z', NULL
          )
        `;

      yield* insert("legacy-linked", "mcp-client", "T3 Code · Laptop");
      yield* insert("ordinary-mcp", "mcp-client", "Claude Desktop");
      yield* insert("same-label-browser", "browser", "T3 Code · Browser", "unknown");

      yield* runMigrations({ toMigrationInclusive: 73 });
      const rows = yield* sql<{ readonly sessionId: string; readonly peerLink: number }>`
        SELECT session_id AS "sessionId", peer_link AS "peerLink"
        FROM auth_sessions
        ORDER BY session_id
      `;
      assert.deepStrictEqual(rows, [
        { sessionId: "legacy-linked", peerLink: 1 },
        { sessionId: "ordinary-mcp", peerLink: 0 },
        { sessionId: "same-label-browser", peerLink: 0 },
      ]);

      // The migration is safe to apply again if an upgrade is interrupted after
      // adding the column but before the backfill commits.
      yield* migrateAuthSessionPeerLink;
      assert.deepStrictEqual(
        yield* sql<{ readonly peerLink: number }>`
          SELECT peer_link AS "peerLink" FROM auth_sessions WHERE session_id = 'legacy-linked'
        `,
        [{ peerLink: 1 }],
      );
    }),
  );
});
