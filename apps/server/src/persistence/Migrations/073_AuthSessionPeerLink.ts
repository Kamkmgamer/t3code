import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(auth_sessions)
  `;

  if (!columns.some((column) => column.name === "peer_link")) {
    yield* sql`
      ALTER TABLE auth_sessions
      ADD COLUMN peer_link INTEGER NOT NULL DEFAULT 0
    `;
  }

  // Before this marker existed, linked environments were identifiable by the
  // MCP client identity they use when signing in, so preserve their durable
  // protection during upgrade.
  yield* sql`
    UPDATE auth_sessions
    SET peer_link = 1
    WHERE subject = 'mcp-client'
      AND method = 'bearer-access-token'
      AND client_device_type = 'bot'
      AND client_label LIKE 'T3 Code · %'
      AND peer_link = 0
  `;
});
