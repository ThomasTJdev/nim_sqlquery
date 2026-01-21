import unittest
import sqlquery/sql_schema_validator



suite "validateColumns - single table":

  test "enums - single table":
    let row = validateColumns(person.id, person.name, person.email)
    check row == @["person.id", "person.name", "person.email"]

  test "enums - single table":
    let row = validateColumns(company.id, company.name)
    check row == @["company.id", "company.name"]

  test "strings - single table":
    let row = validateColumns("company.id", "company.name")
    check row == @["company.id", "company.name"]


suite "validateColumns - multiple tables":

  test "enums - multiple tables":
    let row = validateColumns(person.id, person.name, company.id, company.name)
    check row == @["person.id", "person.name", "company.id", "company.name"]

  test "strings - multiple tables":
    let row = validateColumns("person.id", "person.name", "company.id", "company.name")
    check row == @["person.id", "person.name", "company.id", "company.name"]


suite "validateColumns - mixed input customSQL":

  test "mixed input - enums and strings and custom SQL":
    let row = validateColumns(person.id, "COUNT(company.name)", "SUM(company.creation)", "AVG(company.modified)", "MIN(company.creation)")
    check row == @["person.id", "COUNT(company.name)", "SUM(company.creation)", "AVG(company.modified)", "MIN(company.creation)"]


  test "bad formatting":
    # SCHEMA accepts, but result is not valid. Spacing within funcs.
    let row = validateColumns(person.id, "COUNT(  company.name)", "SUM(   company.creation)")
    check row != @["person.id", "COUNT(company.name)", "SUM(company.creation)"]



suite "validateColumns - mixed input":

  test "mixed input - enums and strings":
    let row = validateColumns(person.id, "person.name", company.id, "company.name")
    check row == @["person.id", "person.name", "company.id", "company.name"]

  test "mixed input - enums and strings and custom SQL":
    let row = validateColumns(person.id, "person.name", company.id, "COUNT(company.name)")
    check row == @["person.id", "person.name", "company.id", "COUNT(company.name)"]



suite "validateColumns - with input values":

  test "string - with input values":
    let row = validateColumns(
      "person.email = 'test@test.com'",
      "person.name = 'test'",
      "person.status = 'test'",
      "person.creation = '2021-01-01'",
      "person.modified = '2021-01-01'",
    )
    check row == @["person.email = 'test@test.com'", "person.name = 'test'", "person.status = 'test'", "person.creation = '2021-01-01'", "person.modified = '2021-01-01'"]


suite "validateColumn":

  test "strings":
    let row = validateColumn("person.id")
    check row == "person.id"

  test "strings":
    let row = validateColumn("person.id")
    check row == "person.id"


suite "validateTableColumns":

  test "strings":
    let row = validateTableColumns("person", ["id", "name", "status", "creation", "modified"])
    check row == @["person.id", "person.name", "person.status", "person.creation", "person.modified"]

  test "strings":
    let row = validateTableColumns("person", ["id", "name", "status"])
    check row == @["person.id", "person.name", "person.status"]


suite "validateInsertColumns":

  test "strings":
    let row = validateInsertColumns("person", ["id", "name", "status", "creation", "modified"])
    check row == @["id", "name", "status", "creation", "modified"]

  test "strings":
    let row = validateInsertColumns("person", ["id", "name", "status"])
    check row == @["id", "name", "status"]


suite "validateTable":

  test "strings":
    let row = validateTable("person")
    check row == "person"

  test "strings":
    let row = validateTable("company")
    check row == "company"

  test "strings":
    let row = validateTable("project")
    check row == "project"

  test "strings":
    let row = validateTable("actions")
    check row == "actions"


suite "db - enum to string":

  test "enum to string":
    let row = db(person.id)
    check row == "person.id"

  test "enum to string":
    let row = db(person.name)
    check row == "person.name"

  test "multiple enums to string":
    let row = @[db(person.id), db(person.name)]
    check row == @["person.id", "person.name"]



suite "wrapper proc":

  test "table":
    let row = table("person")
    check row == "person"

  test "select":
    let row = select(person.id, person.name)
    check row == @["person.id", "person.name"]

  test "select":
    let row = select("person.id", "person.name")
    check row == @["person.id", "person.name"]

  test "select":
    let row = select("person.id", "person.name", company.id, company.name)
    check row == @["person.id", "person.name", "company.id", "company.name"]

  test "where":
    let row = where("person.id = 1", "person.name = 'test'")
    check row == @["person.id = 1", "person.name = 'test'"]

  test "where":
    let row = where("person.id =", "person.name =")
    check row == @["person.id =", "person.name ="]

  test "where":
    let row = where(person.id, person.name)
    check row == @["person.id =", "person.name ="]

  test "insert":
    let row = insert(person.id, person.name, person.status)
    check row == @["id", "name", "status"]

  test "insert":
    let row = insert(person.id, person.name, person.status)
    check row == @["id", "name", "status"]


suite "compile time errors":
  test "invalid enum column name":
    check not compiles(validateColumns(person.idd, person.name))

  test "invalid enum column name in validateColumn":
    check not compiles(validateColumn(person.idd))

  test "invalid enum column name in validateTableColumns":
    check not compiles(validateTableColumns("person", ["idd", "name", "status"]))

  test "invalid enum column name in validateInsertColumns":
    check not compiles(validateInsertColumns("person", ["idd", "name", "status"]))

  test "invalid enum column name in db()":
    check not compiles(db(person.idd))

  test "invalid enum column name in select()":
    check not compiles(select(person.idd, person.name))

  test "invalid enum column name in insert()":
    check not compiles(insert(person.idd, person.name, person.status))

  test "insert":
    let row = insert(person.id, person.name, person.status)
    check row == @["id", "name", "status"]
