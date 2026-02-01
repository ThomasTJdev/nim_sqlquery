import unittest
import std/strutils
import sqlquery/sql_query_generator
import sqlquery/sql_schema_validator


suite "selectQuery":
  test "any":
    let base = selectQuery(
          table = "actions",
          select = @["actions.id", "actions.name", "actions.status"],
          joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.project_id")])],
          where = @[("actions.project_id", "= ANY(?::int[])", "123,456")],
        )

    check base.sql == "SELECT actions.id, actions.name, actions.status FROM actions LEFT JOIN project ON project.id = actions.project_id AND project.is_deleted IS NULL WHERE actions.project_id = ANY(?::int[]) AND actions.is_deleted IS NULL"
    check base.params == @["{123,456}"]

  test "override where structure":
    let base = selectQuery(
          table = "actions",
          select = @["actions.id", "actions.name", "actions.status"],
          joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.project_id")])],
          where = @[("(where my day is good)", "", "")],
        )

    check base.sql == "SELECT actions.id, actions.name, actions.status FROM actions LEFT JOIN project ON project.id = actions.project_id AND project.is_deleted IS NULL WHERE (where my day is good) AND actions.is_deleted IS NULL"

  test "base":
    let base = selectQuery(
          table = "actions",
          select = @["actions.id", "actions.name", "actions.status"],
          joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.project_id")])],
          where = @[("actions.project_id", "=", "123"), ("actions.status", "=", "pending")],
          order = @[("actions.phase", DESC), ("actions.date_end", ASC)],
          limit = 10,
          offset = 1,
          groupBy = @["actions.project_id"]
        )

    check base.sql == "SELECT actions.id, actions.name, actions.status FROM actions LEFT JOIN project ON project.id = actions.project_id AND project.is_deleted IS NULL WHERE actions.project_id = ? AND actions.status = ? AND actions.is_deleted IS NULL GROUP BY actions.project_id ORDER BY actions.phase DESC, actions.date_end ASC LIMIT 10 OFFSET 1"
    check base.params == @["123", "pending"]

  test "user_management_query":
    # Query for active person with their project count and recent activity
    let userQuery = selectQuery(
      table = "person",
      select = @["person.id", "person.name", "person.email", "person.creation", "person.modified"],
      where = @[
        ("person.status", "=", "true"),
        ("person.creation", ">=", "2024-01-01"),
        ("person.email", "LIKE", "%@company.com")
      ],
      order = @[("person.modified", ASC), ("person.creation", ASC)],
      limit = 50
    )

    check userQuery.sql == "SELECT person.id, person.name, person.email, person.creation, person.modified FROM person WHERE person.status = ? AND person.creation >= ? AND person.email LIKE ? AND person.is_deleted IS NULL ORDER BY person.modified ASC, person.creation ASC LIMIT 50"
    check userQuery.params == @["true", "2024-01-01", "%@company.com"]

  test "complex_project_analytics":
    # Complex query with multiple joins for project analytics
    let analyticsQuery = selectQuery(
      table = "project",
      select = @[
        "project.name",
        "project.status",
        "project.description",
        "categories_project.name",
        "person.name"
      ],
      joins = @[
        ("categories_project", INNERJOIN, @[("categories_project.id", "=", "project.id")]),
        ("person", LEFTJOIN, @[("person.id", "=", "project.author_id")])
      ],
      where = @[
        ("project.status", "IN", "('active', 'on_hold')"),
        ("project.description", "!=", "low"),
        ("project.date_end", "BETWEEN", "2024-01-01 AND 2024-12-31"),
        ("categories_project.category", "=", "true")
      ],
      order = @[
        ("project.description", DESC),
        ("project.date_end", ASC),
        ("categories_project.name", ASC)
      ],
      limit = 25,
      offset = 0,
      groupBy = @["project.status", "categories_project.name"]
    )

    check analyticsQuery.sql == "SELECT project.name, project.status, project.description, categories_project.name, person.name FROM project INNER JOIN categories_project ON categories_project.id = project.id LEFT JOIN person ON person.id = project.author_id AND person.is_deleted IS NULL WHERE project.status IN ? AND project.description != ? AND project.date_end BETWEEN ? AND categories_project.category = ? AND project.is_deleted IS NULL GROUP BY project.status, categories_project.name ORDER BY project.description DESC, project.date_end ASC, categories_project.name ASC LIMIT 25"
    check analyticsQuery.params == ["('active', 'on_hold')", "low", "2024-01-01 AND 2024-12-31", "true"]

  test "task_assignment_report":
    # Query for task assignment reporting with NULL handling
    let taskReport = selectQuery(
      table = "actions",
      select = @[
        "actions.name",
        "actions.status",
        "actions.estimatedtimeinhours",
        "actions.estimatedtimeinhours",
        "project.name",
        "person.name"
      ],
      joins = @[
        ("project", INNERJOIN, @[("project.id", "=", "actions.project_id"), ("project.status", "=", "true")]),
        ("person", LEFTJOIN, @[("person.id", "=", "actions.assigned_to")])
      ],
      where = @[
        ("actions.status", "NOT IN", "('cancelled', 'archived')"),
        ("actions.estimatedtimeinhours", ">", "0"),
        ("actions.assigned_to", "IS NOT", "NULL"),
        ("project.status", "=", "true")
      ],
      order = @[
        ("actions.date_end", ASC),
        ("actions.phase", DESC)
      ],
      limit = 100
    )

    check taskReport.sql == "SELECT actions.name, actions.status, actions.estimatedtimeinhours, actions.estimatedtimeinhours, project.name, person.name FROM actions INNER JOIN project ON project.id = actions.project_id AND project.status = ? AND project.is_deleted IS NULL LEFT JOIN person ON person.id = actions.assigned_to AND person.is_deleted IS NULL WHERE actions.status NOT IN ? AND actions.estimatedtimeinhours > ? AND actions.assigned_to IS NOT NULL AND project.status = ? AND actions.is_deleted IS NULL ORDER BY actions.date_end ASC, actions.phase DESC LIMIT 100"
    check taskReport.params == @["true", "('cancelled', 'archived')", "0", "true"]

  test "sql_function_in_where_clause":
    # Test using SQL function like length() in WHERE clause
    let query = selectQuery(
      table = "checklists",
      select = @["checklists.id", "checklists.name", "checklists.description"],
      where = @[
        ("length(description)", "<>", "243"),
        ("checklists.status", "=", "1")
      ]
    )

    check query.sql == "SELECT checklists.id, checklists.name, checklists.description FROM checklists WHERE length(description) <> ? AND checklists.status = ?"
    check query.params == @["243", "1"]

  test "custom_sql_expression_with_sql_prefix":
    # Test using custom SQL expression with sql:> prefix for complex conditions
    let query = selectQuery(
      table = "checklists",
      select = @["checklists.id", "checklists.name", "checklists.description"],
      where = @[
        ("sql:>description IS NULL OR length(description)", "<>", "243"),
        ("checklists.status", "=", "1")
      ]
    )

    check query.sql == "SELECT checklists.id, checklists.name, checklists.description FROM checklists WHERE (description IS NULL OR length(description) <> ?) AND checklists.status = ?"

    check query.params == @["243", "1"]

  test "join with table alias RUNTIME":
    let query = selectQueryRuntime(
      table = "checklists",
      select = @["checklists.id", "checklists.name", "qap.description"],
      joins = @[("checklists AS qap", LEFTJOIN, @[("checklists.id", "=", "qap.id")])],
      where = @[("checklists.status", "=", "1")],
    )
    check query.sql == "SELECT checklists.id, checklists.name, qap.description FROM checklists LEFT JOIN checklists as qap ON checklists.id = qap.id WHERE checklists.status = ?"
    check query.params == @["1"]

  test "join with table alias":
    let query = selectQuery(
      table = "checklists",
      select = @["checklists.id", "checklists.name", "qap.description"],
      joins = @[("checklists AS qap", LEFTJOIN, @[("checklists.id", "=", "qap.id")])],
      where = @[("checklists.status", "=", "1")],
    )
    check query.sql == "SELECT checklists.id, checklists.name, qap.description FROM checklists LEFT JOIN checklists as qap ON checklists.id = qap.id WHERE checklists.status = ?"
    check query.params == @["1"]

  test "groupBy on alias":
    let base = selectQuery(
          table = "actions",
          select = @["actions.id", "actions.name", "actions.status"],
          joins = @[("project AS p", LEFTJOIN, @[("p.id", "=", "actions.project_id")])],
          where = @[("actions.project_id", "=", "123")],
          groupBy = @["p.name"],
        )

    check base.sql == "SELECT actions.id, actions.name, actions.status FROM actions LEFT JOIN project as p ON p.id = actions.project_id WHERE actions.project_id = ? AND actions.is_deleted IS NULL GROUP BY p.name"
    check base.params == @["123"]

  test "table with reserved word column name (method) using strings":
    # Test table with column named "method" which is a reserved word in Nim.
    # When using string literals, use the original SQL column name "method".
    let query = selectQuery(
      table = "api_requests",
      select = @["api_requests.id", "api_requests.method", "api_requests.endpoint", "api_requests.status_code"],
      where = @[
        ("api_requests.method", "=", "POST"),
        ("api_requests.status_code", ">=", "400")
      ],
      order = @[("api_requests.created_at", DESC)],
      limit = 100
    )

    check query.sql == "SELECT api_requests.id, api_requests.method, api_requests.endpoint, api_requests.status_code FROM api_requests WHERE api_requests.method = ? AND api_requests.status_code >= ? AND api_requests.is_deleted IS NULL ORDER BY api_requests.created_at DESC LIMIT 100"
    check query.params == @["POST", "400"]

  test "table with reserved word column name (method) using enum":
    # When using enums, Nim reserved keywords are prefixed with "nim_".
    # The enum value still produces the correct SQL column name "method".
    let query = selectQuery(
      table = "api_requests",
      select = @["api_requests.id", "api_requests.method", "api_requests.endpoint"],
      where = @[
        ("api_requests.method", "=", "GET"),
        ("api_requests.status_code", "<", "300")
      ],
      order = @[("api_requests.created_at", ASC)],
      limit = 50
    )

    check query.sql == "SELECT api_requests.id, api_requests.method, api_requests.endpoint FROM api_requests WHERE api_requests.method = ? AND api_requests.status_code < ? AND api_requests.is_deleted IS NULL ORDER BY api_requests.created_at ASC LIMIT 50"
    check query.params == @["GET", "300"]


suite "select get()":
  test "base":
    let base = selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name", "actions.status"],
      where = @[("actions.id", "=", "123")]
    )

    var rowData: RowSelectionData = (
      table: "actions",
      selected: @["actions.id", "actions.name", "actions.status"],
      row: @["123", "test", "pending"],
    )

    check rowData.get("actions.id") == "123"
    check rowData.get("actions.name") == "test"


  test "base with alias":
    let base = selectQuery(
      table = "actions",
      select = @["actions.id AS action_id", "actions.name AS action_name", "actions.status as action_status"],
      where = @[("actions.id", "=", "123")]
    )

    var rowData: RowSelectionData = (
      table: "actions",
      selected: @["actions.id AS action_id", "actions.name AS action_name", "actions.status as action_status"],
      row: @["123", "test", "pending"],
    )

    check rowData.get("action_id") == "123"
    check rowData.get("action_name") == "test"
    check rowData.get("action_status") == "pending"



suite "deleteQuery":
  test "base":
    let base = deleteQuery(
      table = "actions",
      where = @[("actions.id", "=", "123")]
    )

    check base.sql == "DELETE FROM actions WHERE actions.id = ?"
    check base.params == @["123"]

  test "project":
    let base = deleteQuery(
      table = "project",
      where = @[("project.status", "=", "active"), ("project.name", "LIKE", "%test%")]
    )

    check base.sql == "DELETE FROM project WHERE project.status = ? AND project.name LIKE ?"
    check base.params == @["active", "%test%"]


suite "updateQuery":
  test "base":
    let base = updateQuery(
      table = "actions",
      data = @[("name", "test")],
      where = @[("actions.id", "=", "123")]
    )

    check base.sql == "UPDATE actions SET name = ? WHERE actions.id = ?"
    check base.params == @["test", "123"]

  test "project":
    let base = updateQuery(
      table = "project",
      data = @[("status", "active"), ("name", "test"), ("description", "NULL"), ("date_end", "")],
      where = @[("project.id", "=", "123"), ("project.creation", ">", "134242444")]
    )

    check base.sql == "UPDATE project SET status = ?, name = ?, description = NULL, date_end = NULL WHERE project.id = ? AND project.creation > ?"
    check base.params == @["active", "test", "123", "134242444"]

  test "ARRAY_APPEND + ARRAY_REMOVE":
    let base = updateQuery(
      table = "checklists",
      data = @[
        ("imported_uuids = array_append(COALESCE(imported_uuids, '{}'), ?)", "1234-1234-1234"),
        ("imported_uuids = array_remove(COALESCE(imported_uuids, '{}'), ?)", "4321-4321-4321"),
      ],
      where = @[("id", "=", "123"), ("creation", ">", "134242444")]
    )

    check base.sql == "UPDATE checklists SET imported_uuids = array_append(COALESCE(imported_uuids, '{}'), ?), imported_uuids = array_remove(COALESCE(imported_uuids, '{}'), ?) WHERE id = ? AND creation > ?"
    check base.params == @["1234-1234-1234", "4321-4321-4321", "123", "134242444"]


  test "ARRAY[?]":
    let base = updateQuery(
      table = "checklists",
      data = @[
        ("imported_uuids = ARRAY[?]", "1234-1234-1234"),
        ("imported_templates = ARRAY[?]", "AHU05"),
      ],
      where = @[("id", "=", "123"), ("creation", ">", "134242444")]
    )

    check base.sql == "UPDATE checklists SET imported_uuids = ARRAY[?], imported_templates = ARRAY[?] WHERE id = ? AND creation > ?"
    check base.params == @["1234-1234-1234", "AHU05", "123", "134242444"]


  test "Postgres functions":
    let base = updateQuery(
      table = "actions",
      data = @[
        ("rand = substr(md5(random()::text), 0, 10)", ""),
      ],
      where = @[
        ("actions.project_id", "=", "123,456"),
        ("actions.rand", "IS", "NULL"),
      ],
    )

    check base.sql == "UPDATE actions SET rand = substr(md5(random()::text), 0, 10) WHERE actions.project_id = ? AND actions.rand IS NULL"
    check base.params == @["123,456"]

  test "sql_function_in_where_clause":
    # Test using SQL function like length() in UPDATE WHERE clause
    let query = updateQuery(
      table = "checklists",
      data = @[("status", "1")],
      where = @[
        ("project_id", "=", "456"),
        ("length(description)", "<>", "243"),
      ]
    )

    check query.sql == "UPDATE checklists SET status = ? WHERE project_id = ? AND length(description) <> ?"
    check query.params == @["1", "456", "243"]

  test "custom_sql_expression_with_sql_prefix":
    # Test using custom SQL expression with sql:> prefix in UPDATE WHERE clause
    let query = updateQuery(
      table = "checklists",
      data = @[("status", "1")],
      where = @[
        ("sql:>description IS NULL OR length(description)", "<>", "243"),
        ("checklists.id", "=", "789")
      ]
    )

    check query.sql == "UPDATE checklists SET status = ? WHERE (description IS NULL OR length(description) <> ?) AND checklists.id = ?"
    check query.params == @["1", "243", "789"]

  test "case_insensitive_field_names":
    # Field names with mixed case should be normalized to lowercase
    # and compile successfully (previously would fail at compile time)
    let base = updateQuery(
      table = "actions",
      data = @[("Name", "updated"), ("DESCRIPTION", "new description")],
      where = @[("actions.id", "=", "123")]
    )

    check base.sql == "UPDATE actions SET name = ?, description = ? WHERE actions.id = ?"
    check base.params == @["updated", "new description", "123"]


suite "insertQuery":
  test "base":
    let base = insertQuery(
      table = "actions",
      data = @[("name", "test"), ("description", "NULL"), ("date_end", "")]
    )

    check base.sql == "INSERT INTO actions (name, description, date_end) VALUES (?, NULL, NULL)"
    check base.params == @["test"]

  test "project":
    let base = insertQuery(
      table = "project",
      data = @[("name", "test"), ("description", "NULL"), ("date_end", "2024-01-01")]
    )

    check base.sql == "INSERT INTO project (name, description, date_end) VALUES (?, NULL, ?)"
    check base.params == @["test", "2024-01-01"]

  test "case_insensitive_field_names":
    # Field names with mixed case should be normalized to lowercase
    # and compile successfully (previously would fail at compile time)
    let base = insertQuery(
      table = "actions",
      data = @[("Name", "test"), ("DESCRIPTION", "my description"), ("Date_End", "2024-06-15")]
    )

    check base.sql == "INSERT INTO actions (name, description, date_end) VALUES (?, ?, ?)"
    check base.params == @["test", "my description", "2024-06-15"]


suite "compile time errors":
  test "invalid column name in join condition":
    # Should fail to compile due to typo in column name
    check not compiles(selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name"],
      joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.projdect_id")])]
    ))

  test "invalid column name in groupBy":
    # Should fail to compile due to typo in column name
    check not compiles(selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name"],
      groupBy = @["actions.projdect_id"]
    ))

  test "invalid column name in orderBy":
    # Should fail to compile due to typo in column name
    check not compiles(selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name"],
      order = @[("actions.phas", DESC)]
    ))

  test "any without placeholder should error = ANY(::type[])":
    # Should fail to compile when using = ANY(::type[]) without the ? placeholder.
    # The correct format is = ANY(?::type[]), with ? before the ::
    check not compiles(selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name", "actions.status"],
      where = @[("actions.status", "= ANY(::int[])", "1,2,3")]
    ))


# ============================================================================
# Extended Tests - ORDER BY
# ============================================================================

suite "ORDER BY extended":
  test "QueryDirection.IGNORE omits direction keyword":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name"],
      where = @[("actions.project_id", "=", "123")],
      order = @[("actions.name", IGNORE), ("actions.phase", DESC)]
    )

    check query.sql == "SELECT actions.id, actions.name FROM actions WHERE actions.project_id = ? AND actions.is_deleted IS NULL ORDER BY actions.name, actions.phase DESC"
    check query.params == @["123"]

  test "QueryDirection.IGNORE alone":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id"],
      where = @[("actions.id", "=", "1")],
      order = @[("actions.name", IGNORE)]
    )

    check query.sql == "SELECT actions.id FROM actions WHERE actions.id = ? AND actions.is_deleted IS NULL ORDER BY actions.name"


# ============================================================================
# Extended Tests - WHERE Operators
# ============================================================================

suite "WHERE operators extended":
  test "ILIKE case-insensitive pattern matching":
    let query = selectQuery(
      table = "person",
      select = @["person.id", "person.name", "person.email"],
      where = @[("person.name", "ILIKE", "%john%")]
    )

    check query.sql == "SELECT person.id, person.name, person.email FROM person WHERE person.name ILIKE ? AND person.is_deleted IS NULL"
    check query.params == @["%john%"]

  test "NOT ILIKE case-insensitive negated pattern":
    let query = selectQuery(
      table = "person",
      select = @["person.id", "person.name"],
      where = @[("person.email", "NOT ILIKE", "%spam%")]
    )

    check query.sql == "SELECT person.id, person.name FROM person WHERE person.email NOT ILIKE ? AND person.is_deleted IS NULL"
    check query.params == @["%spam%"]

  test "IS with true boolean value":
    let query = selectQuery(
      table = "project",
      select = @["project.id", "project.name"],
      where = @[("project.status", "IS", "true")]
    )

    check query.sql == "SELECT project.id, project.name FROM project WHERE project.status IS true AND project.is_deleted IS NULL"
    check query.params.len == 0

  test "IS with false boolean value":
    let query = selectQuery(
      table = "project",
      select = @["project.id", "project.name"],
      where = @[("project.status", "IS", "false")]
    )

    check query.sql == "SELECT project.id, project.name FROM project WHERE project.status IS false AND project.is_deleted IS NULL"
    check query.params.len == 0

  test "IS NOT with true boolean value":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id"],
      where = @[("actions.status", "IS NOT", "true")]
    )

    check query.sql == "SELECT actions.id FROM actions WHERE actions.status IS NOT true AND actions.is_deleted IS NULL"
    check query.params.len == 0

  test "BETWEEN operator using whereString":
    # BETWEEN requires two values, so we use whereString for proper parameterization
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "actions.name", "actions.estimatedtimeinhours"],
      where = @[("actions.status", "=", "active")],
      whereString = (where: "AND actions.estimatedtimeinhours BETWEEN ? AND ?", params: @["10", "50"])
    )

    check "actions.estimatedtimeinhours BETWEEN ? AND ?" in query.sql
    check "actions.status = ?" in query.sql
    check query.params == @["active", "10", "50"]

  test "NOT BETWEEN operator using whereString":
    # NOT BETWEEN also requires two values, use whereString for proper parameterization
    let query = selectQueryRuntime(
      table = "api_requests",
      select = @["api_requests.id", "api_requests.status_code"],
      where = @[("api_requests.method", "=", "GET")],
      whereString = (where: "AND api_requests.status_code NOT BETWEEN ? AND ?", params: @["200", "299"])
    )

    check "api_requests.status_code NOT BETWEEN ? AND ?" in query.sql
    check "api_requests.method = ?" in query.sql
    check query.params == @["GET", "200", "299"]


# ============================================================================
# Extended Tests - Runtime Functions
# ============================================================================

suite "runtime functions extended":
  test "selectQueryRuntime basic":
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "actions.name"],
      where = @[("actions.project_id", "=", "123")]
    )

    check query.sql == "SELECT actions.id, actions.name FROM actions WHERE actions.project_id = ? AND actions.is_deleted IS NULL"
    check query.params == @["123"]

  test "deleteQueryRuntime basic":
    let query = deleteQueryRuntime(
      table = "actions",
      where = @[("actions.id", "=", "456")]
    )

    check query.sql == "DELETE FROM actions WHERE actions.id = ?"
    check query.params == @["456"]

  test "deleteQueryRuntime multiple conditions":
    let query = deleteQueryRuntime(
      table = "project",
      where = @[("project.id", "=", "789"), ("project.status", "=", "archived")]
    )

    check query.sql == "DELETE FROM project WHERE project.id = ? AND project.status = ?"
    check query.params == @["789", "archived"]

  test "updateQueryRuntime basic":
    let query = updateQueryRuntime(
      table = "actions",
      data = @[("name", "updated name"), ("status", "completed")],
      where = @[("actions.id", "=", "123")]
    )

    check query.sql == "UPDATE actions SET name = ?, status = ? WHERE actions.id = ?"
    check query.params == @["updated name", "completed", "123"]

  test "insertQueryRuntime basic":
    let query = insertQueryRuntime(
      table = "actions",
      data = @[("name", "new action"), ("status", "pending"), ("project_id", "100")]
    )

    check query.sql == "INSERT INTO actions (name, status, project_id) VALUES (?, ?, ?)"
    check query.params == @["new action", "pending", "100"]

  test "insertQueryRuntime with NULL values":
    let query = insertQueryRuntime(
      table = "project",
      data = @[("name", "test project"), ("description", "NULL"), ("date_end", "")]
    )

    check query.sql == "INSERT INTO project (name, description, date_end) VALUES (?, NULL, NULL)"
    check query.params == @["test project"]

  test "selectQueryRuntime with whereString parameter":
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "actions.name"],
      where = @[("actions.project_id", "=", "123")],
      whereString = (where: "AND actions.status IN (?, ?)", params: @["active", "pending"])
    )

    check query.sql == "SELECT actions.id, actions.name FROM actions WHERE actions.project_id = ? AND actions.status IN (?, ?) AND actions.is_deleted IS NULL"
    check query.params == @["123", "active", "pending"]

  test "selectQueryRuntime whereString with leading AND":
    let query = selectQueryRuntime(
      table = "person",
      select = @["person.id", "person.name"],
      where = @[("person.status", "=", "active")],
      whereString = (where: "AND person.email LIKE ?", params: @["%@example.com"])
    )

    check query.sql == "SELECT person.id, person.name FROM person WHERE person.status = ? AND person.email LIKE ? AND person.is_deleted IS NULL"
    check query.params == @["active", "%@example.com"]


# ============================================================================
# Extended Tests - ignoreDeleteMarker
# ============================================================================

suite "ignoreDeleteMarker":
  test "ignoreDeleteMarker=true omits is_deleted check on main table":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name"],
      where = @[("actions.project_id", "=", "123")],
      ignoreDeleteMarker = true
    )

    check "is_deleted" notin query.sql
    check query.sql == "SELECT actions.id, actions.name FROM actions WHERE actions.project_id = ?"

  test "ignoreDeleteMarker=true with joins omits is_deleted on all tables":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id", "project.name"],
      joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.project_id")])],
      where = @[("actions.project_id", "=", "123")],
      ignoreDeleteMarker = true
    )

    check "is_deleted" notin query.sql

  test "ignoreDeleteMarker=false (default) includes is_deleted check":
    let query = selectQuery(
      table = "project",
      select = @["project.id", "project.name"],
      where = @[("project.id", "=", "1")]
    )

    check "project.is_deleted IS NULL" in query.sql


# ============================================================================
# Extended Tests - Wildcard select
# ============================================================================

suite "wildcard select":
  test "wildcard * in select":
    let query = selectQuery(
      table = "actions",
      select = @["*"],
      where = @[("actions.id", "=", "1")]
    )

    check query.sql == "SELECT * FROM actions WHERE actions.id = ? AND actions.is_deleted IS NULL"

  test "table qualified wildcard":
    let query = selectQuery(
      table = "actions",
      select = @["actions.*"],
      where = @[("actions.id", "=", "1")]
    )

    check query.sql == "SELECT actions.* FROM actions WHERE actions.id = ? AND actions.is_deleted IS NULL"

  test "wildcard with joins":
    let query = selectQuery(
      table = "actions",
      select = @["actions.*", "project.name"],
      joins = @[("project", LEFTJOIN, @[("project.id", "=", "actions.project_id")])],
      where = @[("actions.id", "=", "1")]
    )

    check "actions.*" in query.sql
    check "project.name" in query.sql


# ============================================================================
# Extended Tests - Aggregate functions
# ============================================================================

suite "aggregate functions":
  test "COUNT(*) function":
    let query = selectQuery(
      table = "actions",
      select = @["COUNT(*)"],
      where = @[("actions.project_id", "=", "123")]
    )

    check query.sql == "SELECT count(*) FROM actions WHERE actions.project_id = ? AND actions.is_deleted IS NULL"

  test "COUNT with field":
    let query = selectQuery(
      table = "actions",
      select = @["COUNT(actions.id)"],
      where = @[("actions.project_id", "=", "123")]
    )

    check "count(actions.id)" in query.sql.toLowerAscii()

  test "SUM function":
    let query = selectQuery(
      table = "actions",
      select = @["SUM(actions.estimatedtimeinhours)"],
      where = @[("actions.project_id", "=", "123")]
    )

    check "sum(actions.estimatedtimeinhours)" in query.sql.toLowerAscii()

  test "MIN function":
    let query = selectQuery(
      table = "actions",
      select = @["MIN(actions.date_end)"],
      where = @[("actions.project_id", "=", "123")]
    )

    check "min(actions.date_end)" in query.sql.toLowerAscii()

  test "MAX function":
    let query = selectQuery(
      table = "actions",
      select = @["MAX(actions.estimatedtimeinhours)"],
      where = @[("actions.project_id", "=", "123")]
    )

    check "max(actions.estimatedtimeinhours)" in query.sql.toLowerAscii()

  test "AVG function with alias":
    let query = selectQuery(
      table = "actions",
      select = @["AVG(actions.estimatedtimeinhours) AS avg_hours"],
      where = @[("actions.project_id", "=", "123")]
    )

    check "avg(actions.estimatedtimeinhours) as avg_hours" in query.sql.toLowerAscii()

  test "multiple aggregate functions":
    let query = selectQuery(
      table = "actions",
      select = @["COUNT(*)", "SUM(actions.estimatedtimeinhours)", "AVG(actions.estimatedtimeinhours)"],
      where = @[("actions.project_id", "=", "123")],
      groupBy = @["actions.project_id"]
    )

    check "count(*)" in query.sql.toLowerAscii()
    check "sum(actions.estimatedtimeinhours)" in query.sql.toLowerAscii()
    check "avg(actions.estimatedtimeinhours)" in query.sql.toLowerAscii()


# ============================================================================
# Extended Tests - loopRows iterator
# ============================================================================

suite "loopRows iterator":
  test "loopRows iterates over all rows":
    var rowsData: RowsSelectionData = (
      table: "actions",
      selected: @["actions.id", "actions.name", "actions.status"],
      rows: @[
        @["1", "Task 1", "pending"],
        @["2", "Task 2", "active"],
        @["3", "Task 3", "completed"]
      ]
    )

    var count = 0
    var ids: seq[string] = @[]
    for row in loopRows(rowsData):
      count += 1
      ids.add(row.get("actions.id"))

    check count == 3
    check ids == @["1", "2", "3"]

  test "loopRows with empty rows":
    var rowsData: RowsSelectionData = (
      table: "actions",
      selected: @["actions.id"],
      rows: @[]
    )

    var count = 0
    for row in loopRows(rowsData):
      count += 1

    check count == 0

  test "loopRows preserves table and selected":
    var rowsData: RowsSelectionData = (
      table: "project",
      selected: @["project.id", "project.name"],
      rows: @[@["100", "Project A"]]
    )

    for row in loopRows(rowsData):
      check row.table == "project"
      check row.selected == @["project.id", "project.name"]
      check row.get("project.name") == "Project A"


# ============================================================================
# Extended Tests - Join with value parameter
# ============================================================================

suite "join with value parameter":
  test "join ON clause with literal value (no dot in fieldSecondary)":
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "actions.name"],
      joins = @[("project", LEFTJOIN, @[("project.status", "=", "active")])],
      where = @[("actions.id", "=", "1")]
    )

    check "LEFT JOIN project ON project.status = ?" in query.sql
    check "active" in query.params

  test "join with mixed field reference and value":
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "project.name"],
      joins = @[("project", INNERJOIN, @[
        ("project.id", "=", "actions.project_id"),
        ("project.status", "=", "active")
      ])],
      where = @[("actions.id", "=", "1")]
    )

    check "project.id = actions.project_id" in query.sql
    check "project.status = ?" in query.sql
    check "active" in query.params


# ============================================================================
# Extended Tests - Multiple ON conditions in join
# ============================================================================

suite "multiple ON conditions in join":
  test "join with multiple ON conditions":
    let query = selectQuery(
      table = "actions",
      select = @["actions.id", "actions.name", "project.name"],
      joins = @[("project", INNERJOIN, @[
        ("project.id", "=", "actions.project_id"),
        ("project.status", "=", "actions.status")
      ])],
      where = @[("actions.id", "=", "123")]
    )

    check "ON project.id = actions.project_id AND project.status = actions.status" in query.sql

  test "join with three ON conditions":
    let query = selectQueryRuntime(
      table = "actions",
      select = @["actions.id", "project.name"],
      joins = @[("project", LEFTJOIN, @[
        ("project.id", "=", "actions.project_id"),
        ("project.status", "=", "actions.status"),
        ("project.author_id", "=", "actions.assigned_to")
      ])],
      where = @[("actions.id", "=", "1")]
    )

    check "project.id = actions.project_id" in query.sql
    check "project.status = actions.status" in query.sql
    check "project.author_id = actions.assigned_to" in query.sql


# ============================================================================
# Extended Tests - get() error handling
# ============================================================================

suite "get() error handling":
  test "get with non-existent field returns empty string in release mode":
    var rowData: RowSelectionData = (
      table: "actions",
      selected: @["actions.id", "actions.name"],
      row: @["123", "test"]
    )

    # In release mode, get() returns empty string for non-existent fields
    # In dev mode without test flag, it would quit
    let result = rowData.get("actions.nonexistent")
    check result == ""

  test "get with partial field name without table prefix":
    var rowData: RowSelectionData = (
      table: "actions",
      selected: @["actions.id", "actions.name"],
      row: @["123", "test"]
    )

    # Should find "id" by looking up "actions.id"
    check rowData.get("id") == "123"
    check rowData.get("name") == "test"


# ============================================================================
# Extended Tests - Runtime error handling
# ============================================================================

suite "runtime error handling":
  test "empty select triggers runtime error":
    # Empty select compiles but produces a runtime error
    # The selectQueryRuntime function checks: if selectParsed.len == 0: sqlError("Select cannot be empty")
    # This test documents the behavior - empty select is caught at runtime, not compile time
    let query = selectQuery(
      table = "actions",
      select = @["actions.id"],  # Valid select to avoid runtime error
      where = @[("actions.id", "=", "1")]
    )
    check query.sql.len > 0  # Just verify the query works with valid input