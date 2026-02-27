## Integration test: real PostgreSQL database with schema and query builders.
##
## Requires a running Postgres. Set env vars so the test can connect:
##   TEST_POSTGRES_HOST (default: localhost)
##   TEST_POSTGRES_PORT (default: 5432)
##   TEST_POSTGRES_USER
##   TEST_POSTGRES_PASSWORD
##   TEST_POSTGRES_DB (default: test)
##
## If TEST_POSTGRES_USER is not set, the test is skipped (e.g. when Postgres isn't available).
## In CI, use a Postgres service container and set these env vars in the workflow.

import std/[os, strutils]
import unittest
import waterpark/postgres
import sqlquery/sql_query_generator

# Embed schema at compile time (path relative to project root when nimble compiles).
const SchemaSql = staticRead("sqlschema/schema.sql")

proc getDb(): DbConn =
  let host = getEnv("TEST_POSTGRES_HOST", "127.0.0.1")
  let port = getEnv("TEST_POSTGRES_PORT", "5432")
  let user = getEnv("TEST_POSTGRES_USER", "test")
  let password = getEnv("TEST_POSTGRES_PASSWORD", "test")
  let database = getEnv("TEST_POSTGRES_DB", "test")
  let connStr = if port == "5432": host else: host & ":" & port
  result = open(connStr, user, password, database)
  # Caller must close the connection when done (e.g. test uses defer)


proc runSchema(conn: DbConn) =
  # Each CREATE TABLE ends with ");\n" - split on that so we run complete statements.
  # Splitting on ';' alone would cut inside CREATE TABLE (e.g. after "NULL") and break.
  # Chunks may have leading comments (e.g. "-- Person table..."), so find CREATE TABLE.
  for chunk in SchemaSql.split(");"):
    let idx = chunk.find("CREATE TABLE")
    if idx >= 0:
      let stmt = chunk[idx..^1].strip
      if stmt.len > 0:
        conn.exec(SqlQuery(stmt & ");"))


template withDb(body: untyped) =
  if not existsEnv("TEST_POSTGRES_USER"):
    echo "Skipping Postgres integration test (set TEST_POSTGRES_USER and TEST_POSTGRES_PASSWORD to run)"
    check false
  else:
    let db {.inject.} = getDb()
    defer: close(db)
    runSchema(db)
    body


suite "Postgres integration with schema and builders":
  test "schema applies without error and connection is usable":
    withDb:
      let one = getValue(db, sql("SELECT 1"))
      check one == "1"

  test "insertRow returns new id for person":
    withDb:
      let id = insertRow(
        "person",
        @[("name", "Bob"), ("email", "bob@example.com"), ("status", "active")],
        db = db
      )
      check id > 0

  test "insertRow and selectRows roundtrip":
    withDb:
      let personId = insertRow(
        "person",
        @[("name", "Alice"), ("email", "alice@example.com"), ("status", "active")],
        db = db
      )
      check personId > 0
      let rows = selectRows(
        table = "person",
        select = @["person.id", "person.name", "person.email"],
        where = @[("person.id", "=", $personId)],
        db = db
      )
      check rows.rows.len == 1
      check rows.rows[0][0] == $personId
      check rows.rows[0][1] == "Alice"
      check rows.rows[0][2] == "alice@example.com"

  test "updateValues updates row and selectValueRuntime reads it":
    withDb:
      let personId = insertRow(
        "person",
        @[("name", "Alice"), ("email", "alice@example.com"), ("status", "active")],
        db = db
      )
      let updated = updateValues(
        "person",
        @[("name", "Alice Updated"), ("email", "alice.updated@example.com")],
        @[("person.id", "=", $personId)],
        db = db
      )
      check updated == 1
      let name = selectValueRuntime(
        table = "person",
        select = "person.name",
        where = @[("person.id", "=", $personId)],
        db = db
      )
      check name == "Alice Updated"

  test "updateValue updates single field":
    withDb:
      let personId = insertRow(
        "person",
        @[("name", "Carol"), ("email", "carol@example.com"), ("status", "active")],
        db = db
      )
      let updated = updateValue(
        "person",
        ("status", "inactive"),
        @[("person.id", "=", $personId)],
        db = db
      )
      check updated == 1
      let status = selectValueRuntime(
        table = "person",
        select = "person.status",
        where = @[("person.id", "=", $personId)],
        db = db
      )
      check status == "inactive"

  test "selectRow returns single row with db":
    withDb:
      let personId = insertRow(
        "person",
        @[("name", "Dave"), ("email", "dave@example.com"), ("status", "active")],
        db = db
      )
      let row = selectRow(
        table = "person",
        select = @["person.id", "person.name", "person.email"],
        where = @[("person.id", "=", $personId)],
        db = db
      )
      check row.row.len == 3
      check row.row[0] == $personId
      check row.row[1] == "Dave"
      check row.row[2] == "dave@example.com"

  test "selectRowsRuntime with where returns expected rows":
    withDb:
      let projectId = insertRow(
        "project",
        @[("name", "Runtime Project"), ("status", "open")],
        db = db
      )
      let runtimeRows = selectRowsRuntime(
        table = "project",
        select = @["project.id", "project.name", "project.status"],
        where = @[("project.id", "=", $projectId)],
        db = db
      )
      check runtimeRows.rows.len == 1
      check runtimeRows.rows[0][1] == "Runtime Project"
      check runtimeRows.rows[0][2] == "open"

  test "deleteRows removes row":
    withDb:
      let projectId = insertRow(
        "project",
        @[("name", "To Delete"), ("status", "open")],
        db = db
      )
      let deleted = deleteRows(
        "project",
        @[("project.id", "=", $projectId)],
        db = db
      )
      check deleted == 1
      let after = selectRowsRuntime(
        table = "project",
        select = @["project.id"],
        where = @[("project.id", "=", $projectId)],
        db = db
      )
      check after.rows.len == 0

  test "company table insert and select":
    withDb:
      let companyId = insertRow(
        "company",
        @[("name", "Acme Corp")],
        db = db
      )
      check companyId > 0
      let name = selectValueRuntime(
        table = "company",
        select = "company.name",
        where = @[("company.id", "=", $companyId)],
        db = db
      )
      check name == "Acme Corp"

  test "api_requests table with reserved column name (method)":
    withDb:
      let id = insertRow(
        "api_requests",
        @[
          ("method", "GET"),
          ("endpoint", "/api/health"),
          ("status_code", "200"),
          ("response_time_ms", "12")
        ],
        db = db
      )
      check id > 0
      let rows = selectRowsRuntime(
        table = "api_requests",
        select = @["api_requests.method", "api_requests.endpoint", "api_requests.status_code"],
        where = @[("api_requests.id", "=", $id)],
        db = db
      )
      check rows.rows.len == 1
      check rows.rows[0][0] == "GET"
      check rows.rows[0][1] == "/api/health"
      check rows.rows[0][2] == "200"

  test "person and project: insert both, select project by author":
    withDb:
      let personId = insertRow(
        "person",
        @[("name", "Author"), ("email", "author@example.com"), ("status", "active")],
        db = db
      )
      let projectId = insertRow(
        "project",
        @[("name", "Authored Project"), ("status", "open"), ("author_id", $personId)],
        db = db
      )
      let rows = selectRows(
        table = "project",
        select = @["project.id", "project.name", "project.author_id"],
        where = @[("project.id", "=", $projectId)],
        db = db
      )
      check rows.rows.len == 1
      check rows.rows[0][1] == "Authored Project"
      check rows.rows[0][2] == $personId
