# Copyright Thomas T. Jarløv (TTJ)

import std/[macros]

import waterpark/postgres
import ./sql_query_core

{.push raises: [].}

## Default pool for query execution helpers. Call `initSqlPool` once at startup.
var pg: PostgresPool

proc initSqlPool*(pool: PostgresPool) {.inline.} =
  pg = pool


proc logError(message: string) =
  echo "\e[31mError:\e[0m " & message

proc logWarn(message: string) =
  echo "\e[33mWarning:\e[0m " & message

#
# Query sugar + macros + templates (to-be-macros)
#
macro selectValue*(
  table: static string,
  select: untyped,
  joins: untyped = nil,
  where: untyped = nil,
  order: untyped = nil,
  limit: int = 0,
  offset: int = 0,
  groupBy: untyped = nil,
  ignoreDeleteMarker: bool = false,
  db: DbConn = nil
): untyped =
  result = quote do:
    var selectResult: string
    try:
      var query: QueryResultSelect = selectQuery(`table`, @[`select`], `joins`, `where`, `order`, `limit`, `offset`, `groupBy`, `ignoreDeleteMarker`)
      if `db` != nil:
        selectResult = getValue(`db`, sql(query.sql), query.params)
      else:
        pg.withconnection conn:
          selectResult = getValue(conn, sql(query.sql), query.params)
    except SqlValidationError as e:
      logError(e.msg)
    except SqlQueryWarning as e:
      logError(e.msg)
    except DbError as e:
      logError(e.msg)
    selectResult


macro selectRow*(
  table: static string,
  select: untyped,
  joins: untyped = nil,
  where: untyped = nil,
  order: untyped = nil,
  limit: int = 0,
  offset: int = 0,
  groupBy: untyped = nil,
  ignoreDeleteMarker: bool = false,
  db: DbConn = nil
): untyped =
  result = quote do:
    var selectResult: RowSelectionData
    selectResult.table = `table`
    selectResult.row = @[]
    try:
      var query: QueryResultSelect = selectQuery(`table`, `select`, `joins`, `where`, `order`, `limit`, `offset`, `groupBy`, `ignoreDeleteMarker`)
      selectResult.selected = query.select
      if `db` != nil:
        selectResult.table = `table`
        selectResult.selected = query.select
        selectResult.row = getRow(`db`, sql(query.sql), query.params)
      else:
        pg.withconnection conn:
          selectResult.row = getRow(conn, sql(query.sql), query.params)
    except SqlValidationError as e:
      logError(e.msg)
    except SqlQueryWarning as e:
      logError(e.msg)
    except DbError as e:
      logError(e.msg)
    selectResult


macro selectRows*(
  table: static string,
  select: untyped,
  joins: untyped = nil,
  where: untyped = nil,
  order: untyped = nil,
  limit: int = 0,
  offset: int = 0,
  groupBy: untyped = nil,
  ignoreDeleteMarker: bool = false,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): untyped =
  result = quote do:
    var selectResult: RowsSelectionData
    selectResult.table = `table`
    selectResult.rows = @[]
    try:
      var query: QueryResultSelect = selectQuery(`table`, `select`, `joins`, `where`, `order`, `limit`, `offset`, `groupBy`, `ignoreDeleteMarker`)
      when defined(dev):
        if `debugPrintQuery`:
          echo query.sql
          echo query.params
      if `db` != nil:
        selectResult.table = `table`
        selectResult.selected = query.select
        selectResult.rows = getAllRows(`db`, sql(query.sql), query.params)
      else:
        pg.withconnection conn:
          # We're adding in each loop because each instance must be
          # self-contained.
          selectResult.table = `table`
          selectResult.selected = query.select
          selectResult.rows = getAllRows(conn, sql(query.sql), query.params)
    except SqlValidationError as e:
      logError(e.msg)
    except SqlQueryWarning as e:
      logError(e.msg)
    except DbError as e:
      logError(e.msg)
    selectResult


template updateValue*(
  table: static string,
  data: untyped,
  where: untyped,
  db: DbConn = nil
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = updateQuery(table, @[data], where)
    if db != nil:
      result = execAffectedRows(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = execAffectedRows(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
  except SqlQueryWarning as e:
    logError(e.msg)
  except ValueError as e:
    logError(e.msg)
  except DbError as e:
    logError(e.msg)
  result


template updateValues*(
  table: static string,
  data: untyped,
  where: untyped,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = updateQuery(table, data, where)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params
    if db != nil:
      result = execAffectedRows(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = execAffectedRows(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
  except SqlQueryWarning as e:
    logError(e.msg)
  except DbError as e:
    logError(e.msg)
  except ValueError as e:
    logError(e.msg)
  result


template deleteRows*(
  table: static string,
  where: untyped,
  db: DbConn = nil
): int64 =
  # Convert to template so the literal array syntax gets passed through
  # This enables compile-time field validation even when called from procs
  var result: int64 = 0
  try:
    var query: QueryResult = deleteQuery(table, where)
    if db != nil:
      result = execAffectedRows(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = execAffectedRows(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
  except SqlQueryWarning as e:
    logError(e.msg)
  except ValueError as e:
    logError(e.msg)
  except DbError as e:
    logError(e.msg)
  result


template insertRow*(
  table: static string,
  data: untyped,
  db: DbConn = nil
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = insertQuery(table, data)
    if db != nil:
      result = tryInsertID(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = tryInsertID(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
  except SqlQueryWarning as e:
    logError(e.msg)
  except ValueError as e:
    logError(e.msg)
  except DbError as e:
    logError(e.msg)
  result


proc selectRowsRuntime*(
  table: string,
  select: seq[string],
  joins: seq[JoinSpec] = @[],
  where: seq[WhereSpec] = @[],
  whereString: WhereStringSpec = (where: "", params: @[]),
  order: seq[OrderSpec] = @[],
  limit: int = 0,
  offset: int = 0,
  groupBy: seq[string] = @[],
  ignoreDeleteMarker: bool = false,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): RowsSelectionData =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = select, joins = joins, where = where, whereString = whereString, order = order, limit = limit, offset = offset, groupBy = groupBy, ignoreDeleteMarker = ignoreDeleteMarker)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

    if db != nil:
      result.table = table
      result.selected = query.select
      result.rows = getAllRows(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result.table = table
        result.selected = query.select
        result.rows = getAllRows(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
    return
  except SqlQueryWarning as e:
    logError(e.msg)
    return
  except DbError as e:
    logError(e.msg)
    return

proc selectRowsRuntime*(
  table: string,
  select: seq[string],
  where: WhereNode,
  joins: seq[JoinSpec] = @[],
  whereString: WhereStringSpec = (where: "", params: @[]),
  order: seq[OrderSpec] = @[],
  limit: int = 0,
  offset: int = 0,
  groupBy: seq[string] = @[],
  ignoreDeleteMarker: bool = false,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): RowsSelectionData =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = select, where = where, joins = joins, whereString = whereString, order = order, limit = limit, offset = offset, groupBy = groupBy, ignoreDeleteMarker = ignoreDeleteMarker)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

    if db != nil:
      result.table = table
      result.selected = query.select
      result.rows = getAllRows(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result.table = table
        result.selected = query.select
        result.rows = getAllRows(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
    return
  except SqlQueryWarning as e:
    logError(e.msg)
    return
  except DbError as e:
    logError(e.msg)
    return


proc selectValueRuntime*(
  table: string,
  select: string,
  joins: seq[JoinSpec] = @[],
  where: seq[WhereSpec] = @[],
  whereString: WhereStringSpec = (where: "", params: @[]),
  order: seq[OrderSpec] = @[],
  limit: int = 0,
  offset: int = 0,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): string =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = @[select], joins = joins, where = where, whereString = whereString, order = order, limit = limit, offset = offset)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

    if db != nil:
      result = getValue(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = getValue(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
    return
  except SqlQueryWarning as e:
    logError(e.msg)
    return
  except DbError as e:
    logError(e.msg)
    return

proc selectValueRuntime*(
  table: string,
  select: string,
  where: WhereNode,
  joins: seq[JoinSpec] = @[],
  whereString: WhereStringSpec = (where: "", params: @[]),
  order: seq[OrderSpec] = @[],
  limit: int = 0,
  offset: int = 0,
  debugPrintQuery: bool = false,
  db: DbConn = nil
): string =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = @[select], where = where, joins = joins, whereString = whereString, order = order, limit = limit, offset = offset)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

    if db != nil:
      result = getValue(db, sql(query.sql), query.params)
    else:
      pg.withconnection conn:
        result = getValue(conn, sql(query.sql), query.params)
  except SqlValidationError as e:
    logError(e.msg)
    return
  except SqlQueryWarning as e:
    logError(e.msg)
    return
  except DbError as e:
    logError(e.msg)
    return
