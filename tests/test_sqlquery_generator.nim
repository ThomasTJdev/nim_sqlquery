import unittest
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