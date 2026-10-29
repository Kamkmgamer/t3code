import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(peer_environment_links)
  `;

  if (!columns.some((column) => column.name === "revoked")) {
    yield* sql`
      ALTER TABLE peer_environment_links
      ADD COLUMN revoked INTEGER NOT NULL DEFAULT 0
    `;
  }
});
