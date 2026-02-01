# Copyright Thomas T. Jarløv (TTJ)

import
  std/[
    macros,
    strutils,
    unittest
  ]

import waterpark/postgres
export postgres

# All constants and enums are generated automatically from SQL schema files
# including: tableNamesConstList, fieldEnums and fieldConstList,
# and hasDeleteMarkerFields (for tables with is_deleted field)
import ./sql_schema_validator

{.push raises: [].}

# Custom exception types for SQL validation errors
type
  SqlValidationError* = object of CatchableError
  SqlQueryWarning* = object of CatchableError

# Query result type to hold both SQL and parameters
type
  QueryDirection* = enum
    ASC, DESC, IGNORE

  QueryJoinType* {.pure.} = enum
    INNERJOIN, LEFTJOIN, RIGHTJOIN, FULLJOIN

  QueryResult* = object
    sql*: string
    params*: seq[string]

  QueryResultSelect* = object
    select*: seq[string]
    sql*: string
    params*: seq[string]

type
  JoinSpec* = tuple[table: string, joinType: QueryJoinType, on: seq[tuple[fieldPrimary: string, symbol: string, fieldSecondary: string]]]
  WhereSpec* = tuple[field: string, symbol: string, value: string]
  WhereStringSpec* = tuple[where: string, params: seq[string]]
  OrderSpec* = tuple[field: string, direction: QueryDirection]
  UpdateSpec* = tuple[field: string, value: string]
  InsertSpec* = tuple[field: string, value: string]

type
  RowSelectionData* = tuple[table: string, selected: seq[string], row: seq[string]]
  RowsSelectionData* = tuple[table: string, selected: seq[string], rows: seq[seq[string]]]


var pg*: PostgresPool

proc logError(message: string) =
  echo "\e[31mError:\e[0m " & message

proc logWarn(message: string) =
  echo "\e[33mWarning:\e[0m " & message


#
# Result parsers
#
proc get*(data: RowSelectionData, field: string): string {.raises: [].} =
  let fieldLower = field.toLowerAscii()
  for i, selectedField in data.selected:
    if selectedField == fieldLower:
      return data.row[i]

  if "." notin fieldLower:
    let fieldName = data.table & "." & fieldLower
    for i, selectedField in data.selected:
      if selectedField == fieldName:
        return data.row[i]

  for i, selectedField in data.selected:
    let selectedFieldLower = selectedField.toLowerAscii()
    if selectedFieldLower.contains(" as "):
      let fieldName = selectedFieldLower.split(" as ")
      if fieldName[0] == fieldLower:
        return data.row[i]
      if fieldName[1] == fieldLower:
        return data.row[i]

  when defined(dev):
    logError("Field '" & fieldLower & "' not found in select. Query performed on base table '" & data.table & "'. Available fields: " & data.selected.join(", "))
    quit(1)
  else:
    logError("Field '" & fieldLower & "' not found in select. Query performed on base table '" & data.table & "'. Available fields: " & data.selected.join(", "))
    return ""

iterator loopRows*(data: RowsSelectionData): RowSelectionData {.raises: [].} =
  for i in 0..data.rows.len()-1:
    yield (table: data.table, selected: data.selected, row: data.rows[i])


#
# Helper functions
#
proc sqlError(message: varargs[string, `$`]) =
  let errorMsg = "\e[31mError:\e[0m " & message.join(" ")
  logError errorMsg
  when defined(dev):
    when not defined(test):
      quit(1)
  # raise newException(SqlValidationError, errorMsg)

proc compileError(message: varargs[string, `$`]) =
  error(message.join(" "))

proc sqlWarning(message: varargs[string, `$`]) =
  let warningMsg = "\e[33mWarning:\e[0m " & message.join(" ")
  logWarn warningMsg
  when defined(dev):
    when not defined(test):
      quit(1)
  # raise newException(SqlQueryWarning, warningMsg)


#
# Helper functions
#
proc parseTable(table: string): string =
  let validation = validateTableExists(table)
  if not validation:
    sqlError("Table '" & table & "' does not exist. Valid tables: " & tableNamesConstList.join(", "), table)
  return table


proc parseOrderBy(orderBy: seq[OrderSpec], table = ""): seq[string] =
  for order in orderBy:
    var field = order.field
    if "." notin field and table != "":
      field = table & "." & field

    let direction = order.direction
    if direction != QueryDirection.IGNORE:
      let validation = validateFieldExists(field)
      if not validation.valid:
        sqlError("[ORDER BY] Field '" & validation.fieldName & "' does not exist in table '" & validation.tableName & "'", order)
    if direction notin [QueryDirection.ASC, QueryDirection.DESC, QueryDirection.IGNORE]:
      sqlError("Invalid direction: " & $direction, "Allowed directions: " & $QueryDirection.ASC & ", " & $QueryDirection.DESC, order)
    if direction == QueryDirection.IGNORE:
      result.add(field)
    else:
      result.add(field & " " & $direction)
  return result


proc parseGroupBy(groupBy: seq[string], table = "", tableNamesReal: seq[string] = @[]): seq[string] =
  for group in groupBy:
    var field = group
    if table.len() > 0:
      if "." notin field:
        field = table & "." & field
      else:
        field = table & "." & field.split(".")[1]

      let validation = validateFieldExists(field)
      if validation.valid:
        result.add(group)
        continue
      else:
        if tableNamesReal.len > 0:
          discard
        else:
          sqlError("[GROUP BY] Field '" & validation.fieldName & "' does not exist in table '" & validation.tableName & "'", group)


    # No dot, so it's a table alias
    var found = false
    let groupSplit = group.split(".")
    let groupField = if groupSplit.len > 1: groupSplit[1] else: group
    for tableAlias in tableNamesReal:
      if validateFieldExists(tableAlias & "." & groupField).valid:
        result.add(group)
        found = true
        break
    if not found:
      sqlError("[GROUP BY] Field '" & group & "' as '" & groupField & "' does not exist in any table: " & tableNamesReal.join(", "), group)

  return result


proc parseLimit(limit: int): int =
  if limit < 0:
    sqlError("Limit cannot be negative", limit)
  return limit


proc parseOffset(offset: int): int =
  if offset < 0:
    sqlError("Offset cannot be negative", offset)
  return offset


proc parseWhere(where: seq[WhereSpec], requireTableName = true, table = "", validateSchema = true): tuple[where: seq[string], params: seq[string]] =
  const symbolList = ["=", "!=", ">", "<", ">=", "<=", "<>", "IN", "NOT IN", "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "IS", "IS NOT", "BETWEEN", "NOT BETWEEN"]

  var allTables: seq[string] = @[]
  for i in 0..<where.len:
    let condition = where[i]
    var field = condition[0]
    if field == "project_id" and i > 0:
      sqlWarning("project_id within a where statement will in 99% of cases be the first condition. That's our indexes!")

    let symbol = condition[1]
    let value = condition[2]
    let customSQL = field.startsWith("sql:>")
    var skipRestValidation = false

    if symbol.len() == 0 and value.len() == 0 and not customSQL:
      # We cant skip on customSQL, since we have to remove the sql:> prefix.
      skipRestValidation = true

    #
    # Validation
    #
    if skipRestValidation:
      discard

    elif validateSchema and not customSQL:
      var fieldStr = field
      # Allow to use PostgreSQL functions in where clause, like:
      # ("length(description)", "<>", $templateData[2].len())
      if "(" in fieldStr:
        let s1 = fieldStr.split("(")[1]
        let s2 = s1.split(")")[0]
        # If there's spaces in the function, we skip the rest of the validation
        # since the user can make many custom combinations.
        # Example:
        # MIN(tasks.creation) => Parsed
        # MIN(Lower(tasks.creation) and not null) => Skipped
        if s2.contains(" "):
          skipRestValidation = true
        else:
          fieldStr = s2

      # Allow to skip
      if not skipRestValidation:
        # Combine field with table name if needed
        if not requireTableName and "." notin fieldStr and table != "":
          fieldStr = table & "." & fieldStr

        # Validate the field
        let validation = validateFieldExists(fieldStr)
        if not validation.valid:
          sqlError("[WHERE] Field '" & fieldStr & "' does not exist in table '" & validation.tableName & "'", condition)

        if symbol notin symbolList and not symbol.startsWith("= ANY(?"):
          if symbol == "IN" or symbol == "NOT IN":
            sqlError("IN and NOT IN symbols are not allowed. Use = ANY(?::type[]) instead", condition)
          sqlError("Invalid symbol: " & symbol, "Allowed symbols: " & symbolList.join(", "), condition)

        allTables.add(validation.tableName)

    # Custom SQL ignores all rules and lets you use any SQL you want. Just
    # prepend the field with "sql:>" and it will be ignored by the parser.
    #
    # Example:
    # ("sql:>project_id = ANY({1,2,3})", "", "")
    #
    # This will be treated as a custom SQL statement.
    # That's useful for when you need to use a custom SQL statement that is
    # not supported by the parser.
    elif customSQL:
      field = field.split("sql:>")[1]

    # Manual skip validation
    else:
      if table.len() > 0:
        allTables.add(table)
      else:
        let splitField = field.split(".")
        if splitField.len() == 2:
          allTables.add(splitField[0])

    #
    # Start to format
    #
    let valueLower = value.toLowerAscii()
    var statement: string

    if skipRestValidation:
      statement = field
    elif valueLower == "null":
      statement = field & " " & symbol & " NULL"
    elif valueLower in ["true", "false"] and symbol in ["IS", "IS NOT"]:
      statement = field & " " & symbol & " " & value
    elif symbol.startsWith("= ANY(?"):
      statement = field & " " & symbol
      result.params.add("{" & value & "}")
    elif value.contains("?"):
      statement = field & " " & symbol & " ?"
      result.params.add(value)
    elif customSQL and symbol.len() == 0 and value.len() == 0:
      statement = field
    elif customSQL and symbol.len() == 0:
      statement = field
      result.params.add(value)
    else:
      statement = field & " " & symbol & " ?"
      result.params.add(value)

    # Custom SQL will always be wrapped in parentheses
    if customSQL:
      result.where.add("(" & statement & ")")
    else:
      result.where.add(statement)

  return result


proc parseSelect(select: seq[string], requireTableName = true, table = "", tableAsNames: seq[string] = @[]): seq[string] =
  ## The parse select checks all fields and validates them against the schema.
  ## It returns the input fields 1-1 but in lowercase.
  for field in select:
    let fieldLower = field.toLowerAscii()
    var fieldStr = fieldLower

    # If the field is a wildcard, we add it as is.
    if fieldStr.contains("*"):
      result.add(fieldStr)
      continue

    let fieldSplit = fieldStr.split(" as ")
    if fieldSplit.len > 1:
      fieldStr = fieldSplit[0]

    # A function could be like `MIN(checklist_types.name)`. Here we strip the
    # function parentheses so we can validate the field.
    let isFunction = fieldStr.contains("(") and fieldStr.contains(")")
    if isFunction:
      fieldStr = fieldStr.split("(")[1]
      fieldStr = fieldStr.split(")")[0]

      # If there's a spacing, then there might be a more complex function.
      if fieldStr.contains(" "):
        result.add(fieldLower)
        continue

    if not requireTableName and "." notin fieldStr and table != "":
      if not validateFieldExists(fieldStr).valid and not validateFieldExists(table & "." & fieldStr).valid:
        sqlError("[SELECT] Field '" & fieldStr & "' does not exist 1", field)

    else:
      if not validateFieldExists(fieldStr).valid:
        # Check for alias
        if tableAsNames.len > 0 and fieldStr.contains("."):
          if fieldStr.split(".")[0] notin tableAsNames:
            sqlError("[SELECT] Field '" & fieldStr & "' does not exist 2. Has alias: " & tableAsNames.join(", "), field)
        else:
          sqlError("[SELECT] Field '" & fieldStr & "' does not exist 3", field)

    if isFunction:
      result.add(fieldLower)
    else:
      result.add(fieldLower)

  return result


proc parseJoin(joins: seq[JoinSpec], ignoreDeleteMarker: bool = false): tuple[joins: seq[string], params: seq[string], tableNames: seq[string], tableNamesReal: seq[string]] =

  var previousJoinAlias: seq[tuple[table: string, alias: string]] = @[]

  for join in joins:
    let table = join.table.toLowerAscii()
    let tableSplit = table.split(" as ")
    if tableSplit.len > 1:
      discard parseTable(tableSplit[0])
      result.tableNames.add(tableSplit[1].toLowerAscii())
      result.tableNamesReal.add(tableSplit[0].toLowerAscii())
      previousJoinAlias.add((tableSplit[0].toLowerAscii(), tableSplit[1].toLowerAscii()))
    else:
      discard parseTable(table)
      result.tableNamesReal.add(table.toLowerAscii())

    let joinType = join.joinType
    if joinType notin [QueryJoinType.INNERJOIN, LEFTJOIN, QueryJoinType.RIGHTJOIN, QueryJoinType.FULLJOIN]:
      sqlError("Invalid join type: " & $joinType, "Allowed join types: " & $QueryJoinType.INNERJOIN & ", " & $LEFTJOIN & ", " & $QueryJoinType.RIGHTJOIN & ", " & $QueryJoinType.FULLJOIN, join)

    var onList: seq[string] = @[]
    for on in join.on:
      let fieldPrimary = on.fieldPrimary
      let fieldSecondary = on.fieldSecondary

      # PRIMARY FIELD
      if not validateFieldExists(fieldPrimary).valid:
        # Allow for table alias
        if tableSplit.len > 1 and fieldPrimary.contains("."):
          if not validateFieldExists(tableSplit[0] & "." & fieldPrimary.split(".")[1]).valid:
            sqlError("[JOIN PRIMARY] Field '" & fieldPrimary & "' does not exist in table '" & table & "' 1", on)
        else:
          sqlError("[JOIN PRIMARY] Field '" & fieldPrimary & "' does not exist in table '" & table & "' 2", on)

      # SECONDARY FIELD
      # Only if has a dot, otherwise could be a function or statement
      if fieldSecondary.contains("."):
        var pass: bool = false

        # Raw check
        pass = validateFieldExists(fieldSecondary).valid

        # Use core table
        if not pass and tableSplit.len > 1:
          pass = validateFieldExists(tableSplit[0] & "." & fieldSecondary.split(".")[1]).valid

        # Use previous join alias
        if not pass and previousJoinAlias.len > 0:
          for alias in previousJoinAlias:
            pass = validateFieldExists(alias.table & "." & fieldSecondary.split(".")[1]).valid
            if pass:
              break

        if not pass:
          sqlError("[JOIN SECONDARY] Field '" & fieldSecondary & "' does not exist in table '" & table & "' and previous join aliases was: " & previousJoinAlias.join(", "), on)

      # if not validateFieldExists(fieldSecondary).valid and fieldSecondary.contains("."):
      #   # Check for base table alias
      #   # Check for result.tableNames where previous alias is located
      #   if tableSplit.len > 1 and fieldSecondary.contains("."):
      #     if not validateFieldExists(tableSplit[0] & "." & fieldSecondary.split(".")[1]).valid:
      #       sqlError("[JOIN SECONDARY] Field '" & tableSplit[0] & "." & fieldSecondary.split(".")[1] & "' does not exist in table '" & table & "' 1", on)
      #   else:
      #     sqlError("[JOIN SECONDARY] Field '" & fieldSecondary & "' does not exist in table '" & table & "' 2", on)

      # All pass, include
      if fieldSecondary.contains("."):
        onList.add(fieldPrimary & " " & on.symbol & " " & fieldSecondary)
      else:
        result.params.add(fieldSecondary)
        onList.add(fieldPrimary & " " & on.symbol & " " & "?")

    if table in hasDeleteMarkerFields and not ignoreDeleteMarker:
      onList.add(table & ".is_deleted IS NULL")

    let joinTypeStr = case joinType:
      of QueryJoinType.INNERJOIN: "INNER"
      of LEFTJOIN: "LEFT"
      of QueryJoinType.RIGHTJOIN: "RIGHT"
      of QueryJoinType.FULLJOIN: "FULL"


    result.joins.add($joinTypeStr & " JOIN " & table & " ON " & onList.join(" AND "))


proc parseSetData(table: string, data: seq[UpdateSpec]): tuple[data: seq[string], params: seq[string]] =
  for item in data:
    let field = item.field
    let value = item.value
    var fieldCheck = field
    let fieldHasStatement = field.contains(" = ")
    let fieldHasParameter = field.contains("?")
    if fieldHasStatement:
      fieldCheck = field.split(" = ")[0]

    if "." notin fieldCheck:
      fieldCheck = table & "." & fieldCheck

    let validation = validateFieldExists(fieldCheck)
    if not validation.valid:
      sqlError("[UPDATE] Field '" & fieldCheck & "' does not exist in table '" & validation.tableName & "'", item)

    if fieldHasStatement and not fieldHasParameter:
      result.data.add(field)
    elif fieldHasParameter:
      result.data.add(field)
      result.params.add(value)
    elif value in ["NULL", "null", ""]:
      result.data.add(validation.fieldName & " = NULL")
    else:
      result.data.add(validation.fieldName & " = " & "?")
      result.params.add(value)
  return result


proc parseInsertData(table: string, data: seq[InsertSpec]): tuple[fields: seq[string], values: seq[string], params: seq[string]] =
  for item in data:
    var field = item.field.toLowerAscii()
    let value = item.value
    var validateField = field
    if not field.contains("."):
      validateField = table & "." & field
    else:
      field = field.split(".")[1]
    let validation = validateFieldExists(validateField)
    if not validation.valid:
      sqlError("[INSERT] Field '" & validation.fieldName & "' does not exist in table '" & validation.tableName & "'", item)

    result.fields.add(field)
    if value in ["NULL", "null", ""]:
      result.values.add("NULL")
    else:
      result.values.add("?")
      result.params.add(value)

  return result


#
# Runtime query generators
#
proc selectQueryRuntime*(
  table: string,
  select: seq[string],
  joins: seq[JoinSpec] = @[],
  where: seq[WhereSpec] = @[],
  whereString: WhereStringSpec = (where: "", params: @[]),
  order: seq[OrderSpec] = @[],
  limit: int = 0,
  offset: int = 0,
  groupBy: seq[string] = @[],
  ignoreDeleteMarker: bool = false
): QueryResultSelect =
  let tableParsed = parseTable(table)
  let joinsParsed = parseJoin(joins, ignoreDeleteMarker)

  var selectParsed: seq[string]
  var whereParsed: tuple[where: seq[string], params: seq[string]]
  var groupByParsed: seq[string]
  if joinsParsed.joins.len > 0:
    selectParsed = parseSelect(select, tableAsNames = joinsParsed.tableNames)
    whereParsed = parseWhere(where)
    groupByParsed = parseGroupBy(groupBy, table = tableParsed, tableNamesReal = joinsParsed.tableNamesReal)
  else:
    selectParsed = parseSelect(select, requireTableName = false, table = tableParsed)
    whereParsed = parseWhere(where, requireTableName = false, table = tableParsed)
    groupByParsed = parseGroupBy(groupBy, table = tableParsed)

  let orderParsed = parseOrderBy(order, table = tableParsed)
  let limitParsed = parseLimit(limit)
  let offsetParsed = parseOffset(offset)

  if selectParsed.len == 0:
    sqlError("Select cannot be empty")

  if whereParsed.where.len == 0:
    sqlError("Where cannot be empty")

  var queryParts: seq[string] = @[]
  var queryParams: seq[string] = @[]
  queryParts.add("SELECT " & selectParsed.join(", "))
  queryParts.add("FROM " & tableParsed)

  if joinsParsed.joins.len > 0:
    queryParts.add(joinsParsed.joins.join(" "))
    queryParams.add(joinsParsed.params)

  if whereParsed.where.len > 0:
    queryParts.add("WHERE " & whereParsed.where.join(" AND "))
    queryParams.add(whereParsed.params)

  if whereString.where != "" and whereString.params.len == whereString.where.count("?"):
    if whereParsed.where.len > 0:
      if whereString.where.toLowerAscii().strip().startsWith("and "):
        queryParts.add(whereString.where)
      else:
        queryParts.add("AND " & whereString.where)

    else:
      queryParts.add("WHERE " & whereString.where)
    queryParams.add(whereString.params)

  if tableParsed in hasDeleteMarkerFields and not ignoreDeleteMarker:
    if whereParsed.where.len > 0:
      queryParts.add("AND " & tableParsed & ".is_deleted IS NULL")
    else:
      queryParts.add("WHERE " & tableParsed & ".is_deleted IS NULL")

  if groupByParsed.len > 0:
    queryParts.add("GROUP BY " & groupByParsed.join(", "))

  if orderParsed.len > 0:
    queryParts.add("ORDER BY " & orderParsed.join(", "))

  if limitParsed > 0:
    queryParts.add("LIMIT " & $limitParsed)

  if offsetParsed > 0:
    queryParts.add("OFFSET " & $offsetParsed)

  return QueryResultSelect(select: selectParsed, sql: queryParts.join(" "), params: queryParams)


proc deleteQueryRuntime*(
  table: string,
  where: seq[WhereSpec]
): QueryResult =
  let table = parseTable(table)
  let where = parseWhere(where, requireTableName = false, table = table)

  if where.where.len == 0:
    sqlError("Where cannot be empty")

  var queryParts: seq[string] = @[]
  queryParts.add("DELETE FROM " & table)
  if where.where.len > 0:
    queryParts.add("WHERE " & where.where.join(" AND "))
  return QueryResult(sql: queryParts.join(" "), params: where.params)


proc updateQueryRuntime*(
  table: string,
  data: seq[UpdateSpec],
  where: seq[WhereSpec]
): QueryResult =
  let table = parseTable(table)
  let data = parseSetData(table, data)
  let where = parseWhere(where, requireTableName = false, table = table)

  if data.data.len == 0:
    sqlError("Data cannot be empty")
  if where.where.len == 0:
    sqlError("Where cannot be empty")

  var queryParts: seq[string] = @[]
  var queryParams: seq[string] = @[]
  queryParts.add("UPDATE " & table)
  queryParts.add("SET " & data.data.join(", "))
  queryParams.add(data.params)

  if where.where.len > 0:
    queryParts.add("WHERE " & where.where.join(" AND "))
    queryParams.add(where.params)

  return QueryResult(sql: queryParts.join(" "), params: queryParams)


proc insertQueryRuntime*(
  table: string,
  data: seq[InsertSpec]
): QueryResult =
  let table = parseTable(table)
  let data = parseInsertData(table, data)

  if data.fields.len == 0:
    sqlError("Data cannot be empty")
  if data.fields.len != data.values.len:
    sqlError("Fields and values must have the same length")

  var queryParts: seq[string] = @[]
  queryParts.add("INSERT INTO " & table)
  queryParts.add("(" & data.fields.join(", ") & ")")
  queryParts.add("VALUES (" & data.values.join(", ") & ")")

  return QueryResult(sql: queryParts.join(" "), params: data.params)



#
# Macros
# - Query generation macros
macro selectQuery*(
  table: static string,
  select: untyped, #static seq[string],
  joins: untyped = nil,  # Accept the literal join expression
  where: untyped = nil,
  order: untyped = nil,
  limit: int = 0,
  offset: int = 0,
  groupBy: untyped = nil,
  ignoreDeleteMarker: bool = false
): untyped =

  #
  # :== Validate table
  #
  if not validateTableExists(table):
    compileError("Table '" & table & "' does not exist. Valid tables: " & tableNamesConstList.join(", "))



  #
  # :== Validate joins
  # - Starting with JOINS so any alias is available in select
  #
  var processedJoins = joins
  var processedTable: seq[tuple[table: string, alias: string]] = @[]

  if joins != nil and joins.kind == nnkPrefix and joins[0].eqIdent("@"):
    # Handle @[...] syntax
    let bracketExpr = joins[1]
    if bracketExpr.kind == nnkBracket:
      for joinExpr in bracketExpr:
        var tableName = ""
        var onClauses: seq[NimNode] = @[]
        var hasAlias = false

        if joinExpr.kind == nnkTupleConstr and joinExpr.len > 0:
          # Handle positional syntax: ("table", joinType, @[...])
          let tableNameNode = joinExpr[0]
          if tableNameNode.kind == nnkStrLit:
            tableName = tableNameNode.strVal
            if tableName.toLowerAscii().contains(" as "):
              let split = tableName.toLowerAscii().split(" as ")
              tableName = split[0]
              processedTable.add((table: tableName, alias: split[1]))
              hasAlias = true

            # Extract on clauses if present
            if joinExpr.len >= 3 and joinExpr[2].kind == nnkPrefix and joinExpr[2][0].eqIdent("@"):
              let onBracket = joinExpr[2][1]
              if onBracket.kind == nnkBracket:
                for onExpr in onBracket:
                  onClauses.add(onExpr)

        # Handle named tuple syntax: (table: "name", joinType: ..., on: @[...])
        elif joinExpr.kind == nnkExprColonExpr:
          for field in joinExpr:
            if field.kind == nnkExprColonExpr:
              if field[0].eqIdent("table") and field[1].kind == nnkStrLit:
                tableName = field[1].strVal
                if tableName.toLowerAscii().contains(" as "):
                  let split = tableName.toLowerAscii().split(" as ")
                  tableName = split[0]
                  processedTable.add((table: tableName, alias: split[1]))
                  hasAlias = true
              elif field[0].eqIdent("on") and field[1].kind == nnkPrefix and field[1][0].eqIdent("@"):
                let onBracket = field[1][1]
                if onBracket.kind == nnkBracket:
                  for onExpr in onBracket:
                    onClauses.add(onExpr)

        # Generate validation for join table
        if tableName != "":
          if not validateTableExists(tableName):
            compileError("Join table '" & tableName & "' does not exist. Valid tables: " & tableNamesConstList.join(", "), tableName)

          # Validate ON clause fields
          for onClause in onClauses:
            if onClause.kind == nnkTupleConstr and onClause.len >= 3:
              let primaryField = onClause[0]
              let secondaryField = onClause[2]

              if primaryField.kind == nnkStrLit:
                let primaryFieldStr = primaryField.strVal
                if not validateFieldExists(primaryFieldStr).valid:
                  if hasAlias:
                    let split = primaryFieldStr.split(".")
                    if not validateFieldExists(tableName & "." & split[1]).valid:
                      compileError("[JOIN PRIMARY] Primary field '" & primaryFieldStr & "' does not exist in table 1")
                  else:
                    compileError("[JOIN PRIMARY] Primary field '" & primaryFieldStr & "' does not exist in table 2")

              if secondaryField.kind == nnkStrLit:
                let secondaryFieldStr = secondaryField.strVal
                if secondaryFieldStr.contains("."):
                  if not validateFieldExists(secondaryFieldStr).valid:
                    if hasAlias:
                      let split = secondaryFieldStr.split(".")
                      if not validateFieldExists(tableName & "." & split[1]).valid:
                        compileError("[JOIN SECONDARY] Secondary field '" & secondaryFieldStr & "' does not exist in table 1")
                    else:
                      compileError("[JOIN SECONDARY] Secondary field '" & secondaryFieldStr & "' does not exist in table 2")

  elif joins == nil:
    # Handle nil case - create empty joins
    processedJoins = newCall("@", newNimNode(nnkBracket))


  #
  # :== Validate select
  #
  var processedSelect = select
  if processedSelect != nil and processedSelect.kind == nnkPrefix and processedSelect[0].eqIdent("@"):
    let bracketExpr = processedSelect[1]
    if bracketExpr.kind == nnkBracket:
      for selectExpr in bracketExpr:
        if selectExpr.kind == nnkStrLit:
          var fieldStr = selectExpr.strVal.toLowerAscii()
          if fieldStr.contains("*"):
            continue
          if "." notin fieldStr and joins != nil:
            compileError("[selectQuery - SELECT] Field '" & `fieldStr` & "' is missing table name. Table names are required when using joins.")

          # Potential function
          if "(" in fieldStr and ")" in fieldStr:
            fieldStr = fieldStr.split("(")[1]
            fieldStr = fieldStr.split(")")[0]
            if fieldStr.contains(" "):
              continue

          if "." notin fieldStr and joins == nil:
            fieldStr = table & "." & fieldStr

          if fieldStr.toLowerAscii().contains(" as "):
            fieldStr = fieldStr.toLowerAscii().split(" as ")[0]

          if not validateFieldExists(fieldStr).valid:
            if processedTable.len > 0 and fieldStr.contains("."):
              var pass: bool = false
              let split = fieldStr.split(".")
              let splitTable = split[0]
              let splitField = split[1]

              for table in processedTable:
                if table.alias == splitTable:
                  if validateFieldExists(table.table & "." & splitField).valid:
                    pass = true
                    break
              if not pass:
                compileError("[SELECT] Field '" & fieldStr & "' does not exist 1")
            else:
              compileError("[SELECT] Field '" & fieldStr & "' does not exist 2")


  #
  # :== Validate WHERE clause fields
  #
  var processedWhere = where

  if where != nil and where.kind == nnkPrefix and where[0].eqIdent("@"):
    let bracketExpr = where[1]
    if bracketExpr.kind == nnkBracket:
      for whereExpr in bracketExpr:
        if whereExpr.kind == nnkTupleConstr and whereExpr.len > 0:
          let fieldNode = whereExpr[0]
          if fieldNode.kind == nnkStrLit:
            var fieldStr = fieldNode.strVal
            let customSQL = fieldStr.startsWith("sql:>")
            if customSQL:
              continue

            # Allow to use PostgreSQL functions in where clause, like:
            # ("length(description)", "<>", $templateData[2].len())
            if "(" in fieldStr:
              let s1 = fieldStr.split("(")[1]
              let s2 = s1.split(")")[0]
              if s2.contains(" "):
                continue
              else:
                fieldStr = s2

            if "." notin fieldStr:
              if joins != nil:
                compileError("[selectQuery - WHERE] Where field '" & fieldStr & "' is missing table name. Table names are required when using joins.")
              else:
                fieldStr = table & "." & fieldStr

            if not validateFieldExists(fieldStr).valid:
              compileError("[WHERE] Where field '" & fieldStr & "' does not exist")

          # Some specific scenarios we know will fail, but we support
          # something close to it.
          let symbolNode = whereExpr[1]
          if symbolNode.kind == nnkStrLit:
            let symbolStr = symbolNode.strVal
            if symbolStr.startsWith("= ANY(::"):
              error("= ANY(::) is not supported. Use = ANY(?::type[]) instead")

  elif where == nil:
    # Handle nil case - create empty where
    compileError("Where cannot be empty")
    #processedWhere = newCall("@", newNimNode(nnkBracket))


  #
  # :== Validate ORDER BY clause fields
  #
  var processedOrder = order
  if order != nil and order.kind == nnkPrefix and order[0].eqIdent("@"):
    let bracketExpr = order[1]
    if bracketExpr.kind == nnkBracket:
      for orderExpr in bracketExpr:
        if orderExpr.kind == nnkTupleConstr and orderExpr.len > 0:
          let fieldNode = orderExpr[0]
          if fieldNode.kind == nnkStrLit:
            var fieldStr = fieldNode.strVal
            if "." notin fieldStr and joins == nil:
              fieldStr = table & "." & fieldStr

            if not validateFieldExists(fieldStr).valid:
              compileError("[ORDER BY] Order by field '" & fieldStr & "' does not exist in table '" & table & "'")

  elif order == nil:
    # Handle nil case - create empty order
    processedOrder = newCall("@", newNimNode(nnkBracket))


  #
  # :== Validate GROUP BY clause fields
  #
  var processedGroupBy = groupBy
  if groupBy != nil and groupBy.kind == nnkPrefix and groupBy[0].eqIdent("@"):
    let bracketExpr = groupBy[1]
    if bracketExpr.kind == nnkBracket:
      for groupByExpr in bracketExpr:
        if groupByExpr.kind == nnkStrLit:
          var pass = false
          let groupByStr = groupByExpr.strVal
          if not pass and groupByStr.contains("."):
            if validateFieldExists(groupByStr).valid:
              pass = true

          let groupBySplit = groupByStr.split(".")
          let groupByField = if groupBySplit.len > 1: groupBySplit[1] else: groupByStr

          # Use main table
          if not pass and table.len() > 0:
            if validateFieldExists(table & "." & groupByField).valid:
              pass = true

          # Use table aliases
          if not pass and processedTable.len() > 0:
            for table in processedTable:
              if validateFieldExists(table.table & "." & groupByField).valid:
                pass = true
                break

          if not pass:
            compileError("[GROUP BY] Group by field '" & groupByStr & "' does not exist in table '" & table & "'")

  elif groupBy == nil:
    # Handle nil case - create empty group by
    processedGroupBy = newCall("@", newNimNode(nnkBracket))


  #
  # :== Generate the final call
  #
  result = quote do:
    block:
      selectQueryRuntime(table = `table`, select = `processedSelect`, joins = `processedJoins`, where = `processedWhere`, order = `processedOrder`, limit = `limit`, offset = `offset`, groupBy = `processedGroupBy`, ignoreDeleteMarker = `ignoreDeleteMarker`)


macro deleteQuery*(
  table: static string,
  where: untyped,
): untyped =

  #
  # :== Validate table
  #
  if not validateTableExists(table):
    compileError("Table '" & table & "' does not exist. Valid tables: " & tableNamesConstList.join(", "))

  #
  # :== Validate WHERE clause fields
  #
  var processedWhere = where

  if where != nil and where.kind == nnkPrefix and where[0].eqIdent("@"):
    # Handle @[...] syntax
    let bracketExpr = where[1]
    if bracketExpr.kind == nnkBracket:
      for whereExpr in bracketExpr:
        if whereExpr.kind == nnkTupleConstr and whereExpr.len > 0:
          let fieldNode = whereExpr[0]
          if fieldNode.kind == nnkStrLit:
            var fieldStr = fieldNode.strVal

            if "." notin fieldStr:
              fieldStr = table & "." & fieldStr

            let parts = fieldStr.split(".")
            let fieldTable = parts[0]

            if not validateFieldExists(fieldStr).valid:
              compileError("[deleteQuery - WHERE] Where field '" & fieldStr & "' does not exist. Table: " & table & ".")
            if fieldTable != table:
              compileError("[deleteQuery - WHERE] Where field '" & fieldStr & "' does not exist in table '" & fieldTable & "'")

  elif where == nil:
    # Handle nil case - create empty where
    compileError("Where cannot be empty")

  #
  # :== Generate the final call
  #
  result = quote do:
    block:
      deleteQueryRuntime(`table`, `processedWhere`)


macro updateQuery*(
  table: static string,
  data: untyped,
  where: untyped,
): untyped =

  #
  # :== Validate table
  #
  if not validateTableExists(table):
    compileError("Table '" & table & "' does not exist. Valid tables: " & tableNamesConstList.join(", "))

  #
  # :== Validate data
  #
  var processedData = data

  if data != nil and data.kind == nnkPrefix and data[0].eqIdent("@"):
    # Handle @[...] syntax
    let bracketExpr = data[1]
    if bracketExpr.kind == nnkBracket:
      for dataExpr in bracketExpr:
        if dataExpr.kind == nnkTupleConstr and dataExpr.len > 0:
          let fieldNode = dataExpr[0]
          if fieldNode.kind == nnkStrLit:
            var fieldStr = fieldNode.strVal

            if " = " in fieldStr:
              fieldStr = fieldStr.split(" = ")[0]

            if "." in fieldStr:
              compileError("[updateQuery - UPDATE] Update set includes table name when setting field '" & fieldStr & "'. The values in update data may not contain table name.")

            var fieldName = table & "." & fieldStr
            if not validateFieldExists(fieldName).valid:
              compileError("[updateQuery - UPDATE] Data field '" & fieldStr & "' does not exist in table '" & table & "'")

  elif data == nil:
    # Handle nil case - create empty data
    compileError("Data cannot be empty")

  #
  # :== Validate where
  #
  var processedWhere = where

  # Update does not have joins (unless you wants to be screwed), so we allow
  # skipping table name in where clause
  if where != nil and where.kind == nnkPrefix and where[0].eqIdent("@"):
    # Handle @[...] syntax
    let bracketExpr = where[1]
    if bracketExpr.kind == nnkBracket:
      for whereExpr in bracketExpr:
        if whereExpr.kind == nnkTupleConstr and whereExpr.len > 0:
          let fieldNode = whereExpr[0]
          if fieldNode.kind == nnkStrLit:
            var fieldStr = fieldNode.strVal
            let customSQL = fieldStr.startsWith("sql:>")
            if customSQL:
              continue

            # Allow to use PostgreSQL functions in where clause, like:
            # ("length(description)", "<>", $templateData[2].len())
            if "(" in fieldStr:
              let s1 = fieldStr.split("(")[1]
              let s2 = s1.split(")")[0]
              fieldStr = s2
              if fieldStr.contains(" "):
                continue

            if "." notin fieldStr:
              fieldStr = table & "." & fieldStr

            let parts = fieldStr.split(".")
            let fieldTable = parts[0]

            if not validateFieldExists(fieldStr).valid:
              compileError("[updateQuery - WHERE] Where field '" & fieldStr & "' does not exist. Table: " & table & ".")
            if fieldTable != table:
              compileError("[updateQuery - WHERE] Where field '" & fieldStr & "' does not exist in table '" & fieldTable & "'")
  elif where == nil:
    # Handle nil case - create empty where
    compileError("Where cannot be empty")

  #
  # :== Generate the final call
  #
  result = quote do:
    block:
      updateQueryRuntime(`table`, `processedData`, `processedWhere`)


macro insertQuery*(
  table: static string,
  data: untyped,
): untyped =

  #
  # :== Validate table
  #
  if not validateTableExists(table):
    compileError("Table '" & table & "' does not exist. Valid tables: " & tableNamesConstList.join(", "))

  #
  # :== Validate data
  #
  var processedData = data

  if data != nil and data.kind == nnkPrefix and data[0].eqIdent("@"):
    # Handle @[...] syntax
    let bracketExpr = data[1]
    if bracketExpr.kind == nnkBracket:
      for dataExpr in bracketExpr:
        if dataExpr.kind == nnkTupleConstr and dataExpr.len > 0:
          let fieldNode = dataExpr[0]
          if fieldNode.kind == nnkStrLit:
            let fieldStr = fieldNode.strVal.toLowerAscii()
            let parts = fieldStr.split(".")
            var fieldName = fieldStr
            if parts.len == 1:
              fieldName = table & "." & fieldStr
            else:
              sqlWarning("[insertQuery - INSERT] SQL WARNING: Insert set includes table name when setting field '" & fieldStr & "'. The values in insert data may not contain table name.")

            if not validateFieldExists(fieldName).valid:
              compileError("[insertQuery - INSERT] Data field '" & fieldStr & "' does not exist in table '" & table & "'")

  elif data == nil:
    # Handle nil case - create empty data
    compileError("Data cannot be empty")

  #
  # :== Generate the final call
  #
  result = quote do:
    block:
      insertQueryRuntime(`table`, `processedData`)



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
  ignoreDeleteMarker: bool = false
): untyped =
  result = quote do:
    var selectResult: string
    try:
      var query: QueryResultSelect = selectQuery(`table`, @[`select`], `joins`, `where`, `order`, `limit`, `offset`, `groupBy`, `ignoreDeleteMarker`)
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
  ignoreDeleteMarker: bool = false
): untyped =
  result = quote do:
    var selectResult: RowSelectionData
    selectResult.table = `table`
    selectResult.row = @[]
    try:
      var query: QueryResultSelect = selectQuery(`table`, `select`, `joins`, `where`, `order`, `limit`, `offset`, `groupBy`, `ignoreDeleteMarker`)
      selectResult.selected = query.select
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
  debugPrintQuery: bool = false
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
  where: untyped
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = updateQuery(table, @[data], where)
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
  debugPrintQuery: bool = false
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = updateQuery(table, data, where)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params
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
  where: untyped
): int64 =
  # Convert to template so the literal array syntax gets passed through
  # This enables compile-time field validation even when called from procs
  var result: int64 = 0
  try:
    var query: QueryResult = deleteQuery(table, where)
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
  data: untyped
): int64 =
  var result: int64 = 0
  try:
    var query: QueryResult = insertQuery(table, data)
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
  debugPrintQuery: bool = false
): RowsSelectionData =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = select, joins = joins, where = where, whereString = whereString, order = order, limit = limit, offset = offset, groupBy = groupBy, ignoreDeleteMarker = ignoreDeleteMarker)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

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
  debugPrintQuery: bool = false
): string =
  var query: QueryResultSelect
  try:
    query = selectQueryRuntime(table = table, select = @[select], joins = joins, where = where, whereString = whereString, order = order, limit = limit, offset = offset)
    when defined(dev):
      if debugPrintQuery:
        echo query.sql
        echo query.params

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
