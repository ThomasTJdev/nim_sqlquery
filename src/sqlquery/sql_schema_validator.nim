# Copyright Thomas T. Jarløv (TTJ)
##
## This file reads SQL schema files and generates Nim enums from the table columns.
## It then provides a macro to validate column names and enum values at compile time.
## Static type checking is used to ensure that only valid column names are used in queries.
##

when NimMajor >= 2:
  import std/[
    macros, dirs, paths, strutils, sequtils
  ]
else:
  import std/[
    macros, os, strutils, sequtils
  ]


const SqlSchemaPath {.strdefine.} = "resources/sql/schema"
const SqlSchemaSoftDeleteMarker {.strdefine.} = "is_deleted"

macro generateEnumsFromSQL(dir: static string): untyped =
  ## Reads SQL schema files and generates Nim enums from the table columns.
  ## Returns a Nim AST containing the enum definitions.

  var sqlFiles: seq[string]
  when NimMajor >= 2:
    for p in walkDir(Path(dir)):
      let path = $p.path
      if path.endsWith(".sql"):
        sqlFiles.add(path)
  else:
    for file in walkDir(dir):
      if (file.path).endsWith(".sql"):
        sqlFiles.add(file.path)

  if sqlFiles.len == 0:
    error("No SQL files found in directory: " & dir)
  else:
    echo "Found SQL files: " & $sqlFiles

  var result = newStmtList()
  var tableNames: seq[string]
  var tableFields: seq[tuple[tableName: string, fields: seq[string]]]
  var tablesWithDeleteMarker: seq[string]


  #
  # Create a type section to hold all enum definitions
  #
  var typeSection = newNimNode(nnkTypeSection)

  #
  # Enums
  #
  for file in sqlFiles:
    let sql = readFile(file)
    for statement in sql.split(";"):
      var trimmedStmt: string
      var commentBlockActive = false

      for line in statement.strip().strip(chars={'\n'}).splitLines():
        if line.strip().len == 0: continue
        if line.strip().startswith("--"): continue
        if line.strip().startswith("*/"):
          commentBlockActive = false
          continue
        if commentBlockActive: continue
        if line.strip().startswith("/*"):
          if line.strip().endsWith("*/"):
            continue
          commentBlockActive = true
          continue
        if line.contains("/*") and not line.contains("*/"):
          commentBlockActive = true
        if toLowerAscii(line).contains("foreign"): continue
        trimmedStmt.add(line.strip().replace("\"", "") & "\n")

      trimmedStmt = trimmedStmt.toLowerAscii()

      if not trimmedStmt.startsWith("create table"): continue

      let tableNameStart = trimmedStmt.find("table") + 6
      let tableNameEnd = trimmedStmt.find("(", tableNameStart)
      if tableNameStart == -1 or tableNameEnd == -1: continue

      let tableName = trimmedStmt[tableNameStart..<tableNameEnd].strip()
        .replace("if not exists", "")
        .replace("IF NOT EXISTS", "")
        .strip()

      if tableName.len == 0: continue
      #tableNames.add("db" & tableName)
      tableNames.add(tableName)

      let startIdx = trimmedStmt.find("(")
      let endIdx = trimmedStmt.rfind(")")
      if startIdx == -1 or endIdx == -1: continue

      let columnsBlock = trimmedStmt[startIdx+1 ..< endIdx].strip
      var columns: seq[string]

      for line in columnsBlock.splitLines():
        let cleanLine = line.strip().split(" ")[0]
        if cleanLine.len > 0 and cleanLine notin ["FOREIGN", "PRIMARY", "KEY", "REFERENCES"]:
          columns.add(cleanLine)

      if columns.len == 0: continue

      # Check if this table has an is_deleted field
      if SqlSchemaSoftDeleteMarker in columns:
        tablesWithDeleteMarker.add(tableName)

      # Store table fields for const generation
      tableFields.add((tableName, columns))

      # Create enum definition
      let enumName = ident(tableName)
      var enumDef = newNimNode(nnkTypeDef)

      # Create a pragma node for the pure pragma
      var pragmaNode = newNimNode(nnkPragmaExpr)
      pragmaNode.add(postfix(enumName, "*"))

      var pragmas = newNimNode(nnkPragma)
      pragmas.add(ident("pure"))
      pragmaNode.add(pragmas)

      enumDef.add(pragmaNode)  # Add the pragma expression
      enumDef.add(newEmptyNode())

      var enumTy = newNimNode(nnkEnumTy)
      enumTy.add(newEmptyNode())

      for col in columns:
        # ! For now skip here. Nim does not allow for keyword `method`
        if col == "method":
          continue
        enumTy.add(newTree(nnkEnumFieldDef,
          ident(col),
          newLit(tableName & "." & col)
        ))

      enumDef.add(enumTy)
      typeSection.add(enumDef)


  #
  # Add objects for each table
  #
  for tableField in tableFields:
    let tableName = tableField.tableName
    let fields = tableField.fields

    let objectTypeName = ident(capitalizeAscii(tableName) & "Object")
    var objectFields = newNimNode(nnkRecList)

    for field in fields:
      if field == "method": continue # Skip Nim keywords

      # Determine field type based on common patterns
      var fieldType = ident("string") # Default to string
      if field.endsWith("_id") or field == "id":
        fieldType = ident("int")
      #elif field.contains("created") or field.contains("updated"):
      #  fieldType = ident("DateTime")
      elif field.contains("is_") or field == "active":
        fieldType = ident("bool")

      objectFields.add(newNimNode(nnkIdentDefs).add(
        postfix(ident(field), "*"),
        fieldType,
        newEmptyNode()
      ))

    let objectTypeDef = newNimNode(nnkTypeDef).add(
      postfix(objectTypeName, "*"),
      newEmptyNode(),
      newNimNode(nnkObjectTy).add(
        newEmptyNode(),
        newEmptyNode(),
        objectFields
      )
    )

    typeSection.add(objectTypeDef)


  result.add(typeSection)


  #
  # Add table names const
  #
  # const
  #   tableNamesConstList* = ["users", "categories", "projects", "tasks", "comments"]
  let tableNamesNode = newTree(
      nnkConstSection,
      newTree(
        nnkConstDef,
        postfix(ident("tableNamesConstList"), "*"),
        newEmptyNode(),
        newTree(
          nnkBracket,
          tableNames.mapIt(newLit(it))
        )
      )
    )
  result.add(tableNamesNode)


  #
  # Add field constants for each table
  #
  # const
  #   usersSqlFields* = ["id", "username", "email", "password_hash", "first_name",
  #     "last_name", "is_active", "created_at", "updated_at"]
  var fieldConstSection = newNimNode(nnkConstSection)
  for tableField in tableFields:
    let tableName = tableField.tableName
    let fields = tableField.fields

    # Create const name like "usersFields*"
    let constName = postfix(ident(tableName & "SqlFields"), "*")

    # Create array of field names
    var fieldArray = newNimNode(nnkBracket)
    for field in fields:
      # Skip the "method" field as mentioned in the original code
      if field == "method":
        continue
      fieldArray.add(newLit(field))

    # Create the const definition
    let fieldConstDef = newTree(
      nnkConstDef,
      constName,
      newEmptyNode(),
      fieldArray
    )

    fieldConstSection.add(fieldConstDef)

  result.add(fieldConstSection)


  #
  # Add hasDeleteMarkerFields const
  #
  # const
  #   hasDeleteMarkerFields* = ["projects"]
  if tablesWithDeleteMarker.len > 0:
    var deleteMarkerConstSection = newNimNode(nnkConstSection)

    # Create array of table names with delete markers
    var deleteMarkerArray = newNimNode(nnkBracket)
    for tableName in tablesWithDeleteMarker:
      deleteMarkerArray.add(newLit(tableName))

    # Create the const definition
    let deleteMarkerConstDef = newTree(
      nnkConstDef,
      postfix(ident("hasDeleteMarkerFields"), "*"),
      newEmptyNode(),
      deleteMarkerArray
    )

    deleteMarkerConstSection.add(deleteMarkerConstDef)
    result.add(deleteMarkerConstSection)

  #
  # Generate validateTableExists proc first
  #
  var validateTableExistsProc = newProc(
    postfix(ident("validateTableExists"), "*"),
    [
      ident("bool"),
      newIdentDefs(ident("tableName"), ident("string"))
    ]
  )

  #
  # Generate body: tableName in tableNamesConstList
  #
  var tableExistsProcBody = newStmtList()
  tableExistsProcBody.add(
    newNimNode(nnkInfix).add(
      ident("in"),
      ident("tableName"),
      ident("tableNamesConstList")
    )
  )
  validateTableExistsProc.body = tableExistsProcBody
  result.add(validateTableExistsProc)

  #
  # Generate validateFieldExists proc
  #
  var validateFieldExistsProc = newProc(
    postfix(ident("validateFieldExists"), "*"),
    [
      # Return type - use parseExpr to parse the tuple type
      parseExpr("tuple[valid: bool, tableName: string, fieldName: string]"),
      # Parameters
      newIdentDefs(ident("field"), ident("string"))
    ]
  )

  # Generate proc body
  var procBody = newStmtList()

  # Add field parsing logic
  procBody.add(parseStmt("""
let parts = field.split(".")
if parts.len != 2:
  return (false, "", "")
let tableName = parts[0]
let fieldName = parts[1]

# Check if table exists
if not validateTableExists(tableName):
  return (false, tableName, fieldName)
  """))

  # Generate case statement for field validation
  var caseStmt = newNimNode(nnkCaseStmt)
  caseStmt.add(ident("tableName"))

  # Add case branches for each table
  for tableField in tableFields:
    let tableName = tableField.tableName
    let fieldConstName = tableName & "SqlFields"

    var ofBranch = newNimNode(nnkOfBranch)
    ofBranch.add(newLit(tableName))

    # Create return statement: return (fieldName in tableNameSqlFields, tableName, fieldName)
    var returnExpr = newNimNode(nnkTupleConstr)
    returnExpr.add(
      newNimNode(nnkInfix).add(
        ident("in"),
        ident("fieldName"),
        ident(fieldConstName)
      )
    )
    returnExpr.add(ident("tableName"))
    returnExpr.add(ident("fieldName"))

    ofBranch.add(newNimNode(nnkReturnStmt).add(returnExpr))
    caseStmt.add(ofBranch)

  # Add else branch
  var elseBranch = newNimNode(nnkElse)
  var elseReturn = newNimNode(nnkTupleConstr)
  elseReturn.add(newLit(false))
  elseReturn.add(ident("tableName"))
  elseReturn.add(ident("fieldName"))
  elseBranch.add(newNimNode(nnkReturnStmt).add(elseReturn))
  caseStmt.add(elseBranch)

  procBody.add(caseStmt)
  validateFieldExistsProc.body = procBody

  result.add(validateFieldExistsProc)



  # echo "\n##################"
  # echo "AST:\n"
  # echo result.treeRepr
  # echo "##################\n"
  #[

  Output is:

  type
    users* {.pure.} = enum
      id = "users.id", username = "users.username", email = "users.email",
      password_hash = "users.password_hash", first_name = "users.first_name",
      last_name = "users.last_name", is_active = "users.is_active",
      created_at = "users.created_at", updated_at = "users.updated_at"
    categories* {.pure.} = enum
      id = "categories.id", name = "categories.name",
      description = "categories.description", color = "categories.color",
      is_active = "categories.is_active", created_at = "categories.created_at",
      updated_at = "categories.updated_at"

  const
    tableNamesConstList* = ["users", "categories"]

  const
    usersFields* = ["id", "username", "email", "password_hash", "first_name", "last_name", "is_active", "created_at", "updated_at"]
    categoriesFields* = ["id", "name", "description", "color", "is_active", "created_at", "updated_at"]
    hasDeleteMarkerFields* = ["projects"]  # Only tables with is_deleted field
  ]#

  when defined(verboseSQL):
    echo "\n##################"
    echo "SQL enumeration:\n"
    echo result.repr
    echo "##################\n"

  return result


generateEnumsFromSQL(SqlSchemaPath)


macro generateBindSymList(arr: static openArray[string]): untyped =
  ## Generates `[bindSym("person"), bindSym("company_notes")]` since I couldn't
  ## figure out how to generate a Nim AST sequence directly.
  var listNode = newNimNode(nnkBracket)
  for enumName in arr:
    let bindCall = newCall("bindSym", newLit(enumName))
    listNode.add(bindCall)
  return listNode


proc db*[T: enum](e: T): string =
  ## Converts an enum value to its string representation.
  return $e


macro validateColumn*(col: string): string =
  ## Wrapper around validateColumns for single column validation.
  ## Returns the validated column string.
  let validated = newCall("validateColumns", col)
  result = newTree(
    nnkBracketExpr,
    validated,
    newLit(0)
  )


macro validateColumns*(args: varargs[untyped]): untyped =
  ## Macro to convert enums to their string values at compile time.
  ## Returns a seq[string] containing the converted values.
  ## Validates string literals against available enum values.
  result = newStmtList()

  var bracketNode = newNimNode(nnkBracket)

  # Collect all valid column names and enum types
  var validColumns = newSeq[string]()
  var enumTypes = newSeq[string]()

  # Helper to process enum types
  proc processEnumType(sym: NimNode) =
    let impl = sym.getImpl
    if impl != nil and impl.kind == nnkTypeDef:
      let typeImpl = impl[2]  # Get the type implementation
      if typeImpl.kind == nnkEnumTy:
        enumTypes.add($sym)
        for value in typeImpl:
          if value.kind == nnkEnumFieldDef:
            validColumns.add(value[1].strVal)

  let boundEnums = generateBindSymList(tableNamesConstList)
  for p in boundEnums:
    try:
      processEnumType(p)
    except:
      discard


  proc extractTableAndField(col: string): tuple[table, field: string] =
    let parts = col.split('.')
    if parts.len == 2:
      return (parts[0], parts[1])
    return ("", "")

  proc validateColumn(col: string): bool =
    let (table, field) = extractTableAndField(col)
    if table.len == 0 or field.len == 0:
      return false
    return col in validColumns

  proc parseAndValidateFunction(funcStr: string): bool =
    # Extract function name and arguments
    let openParen = funcStr.find('(')
    let closeParen = funcStr.rfind(')')
    if openParen == -1 or closeParen == -1:
      return false

    let args = funcStr[openParen + 1 .. closeParen - 1]

    # For each argument, validate if it's a column reference
    for arg in args.split(','):
      let cleanArg = arg.strip()
      # Skip if it's a literal or special character
      if cleanArg == "*" or cleanArg.contains("'") or cleanArg.contains("\""):
        continue

      # If it looks like a column reference, validate it
      if cleanArg.contains('.'):
        if not validateColumn(cleanArg):
          error("Invalid column reference in function: " & cleanArg &
                "\nAvailable columns are: " & $validColumns)
                #lineInfoObj(funcStr))
          return false

    return true

  proc processArg(arg: NimNode) =
    case arg.kind
    of nnkStrLit:
      let colName = arg.strVal

      # Check if it's a function call
      if colName.contains('('):
        if not parseAndValidateFunction(colName):
          error("Invalid function format: " & colName, arg)

      # Check if it's an alias, same as space below
      elif colName.contains(" AS "):
        let parts = colName.split(" AS ")
        if parts.len != 2:
          error("Invalid column alias format: " & colName, arg)
        let (table, field) = extractTableAndField(parts[0])
        if table.len == 0 or field.len == 0:
          error("Invalid column alias format: " & colName, arg)
        if not validateColumn(parts[0]):
          error("Invalid column name: " & parts[0] &
                "\nAvailable columns are: " & $validColumns &
                "\nFrom enums: " & $enumTypes, arg)

      # If we're using it on "where xx =", there we have "xx =" or "xx IS NULL"
      elif colName.contains(" "):
        let parts = colName.split(" ")
        if parts.len < 2:
          error("Invalid column format: " & colName, arg)
        if not validateColumn(parts[0]):
          error("Invalid column name: " & parts[0] &
                "\nAvailable columns are: " & $validColumns &
                "\nFrom enums: " & $enumTypes, arg)

      # Default
      else:
        if not validateColumn(colName):
          error("Invalid column name: " & colName &
                "\nAvailable columns are: " & $validColumns &
                "\nFrom enums: " & $enumTypes, arg)
      bracketNode.add(arg)
    else:  # Convert enums using `db()`
      bracketNode.add(newCall("db", arg))

  for arg in args:
    if arg.kind == nnkPrefix and arg[0].eqIdent("@"):  # Detect if it's a sequence `@[]`
      let bracketExpr = arg[1]
      for subArg in bracketExpr:
        processArg(subArg)
    else:
      processArg(arg)

  # Wrap the bracket node in a sequence constructor
  result = newNimNode(nnkPrefix)
  result.add(newIdentNode("@"))
  result.add(bracketNode)


macro validateTableColumns*(tableName: static[string], columns: static[openArray[string]]): untyped =
  ## This macro wraps `validateColumns` and ensures it returns a `seq[string]`
  result = newStmtList()
  var columnSeqFull = newNimNode(nnkBracket)  # Full names for validateColumns()

  for col in columns:
    columnSeqFull.add(newLit(tableName & "." & col))  # Full names for validateColumns()

  result.add(quote do: discard validateColumns(@`columnSeqFull`))  # Validate and discard result
  result.add(quote do: @`columnSeqFull`)  # Return only column names


macro validateInsertColumns*(tableName: static[string], columns: static[openArray[string]]): untyped =
  ## This macro wraps `validateColumns` and ensures it returns a `seq[string]`.
  ## The input takes the table name and a seq[string] of column names. The
  ## output is a seq[string] of column names without the table name so it can
  ## be used in an insert statement.
  result = newStmtList()
  var columnSeqFull = newNimNode(nnkBracket)  # Full names for validateColumns()
  var columnSeqShort = newNimNode(nnkBracket)  # Only column names for return value

  for col in columns:
    columnSeqFull.add(newLit(tableName & "." & col))  # Full names for validateColumns()
    columnSeqShort.add(newLit(col))  # Only column names for return

  result.add(quote do: discard validateColumns(@`columnSeqFull`))  # Validate and discard result
  result.add(quote do: @`columnSeqShort`)  # Return only column names


macro validateTable*(table: static string): untyped =
  ## Validates if an enum with the given table name exists and returns it.

  if table in tableNamesConstList:
    result = newLit(table)
  else:
    error("Table not found: " & table &
          "\nAvailable tables are: " & $tableNamesConstList)


proc table*(name: static string): string =
  ## Wrapper proc that validates and returns the table name
  return validateTable(name)

template select*(columns: varargs[untyped]): seq[string] =
  ## Template wrapper that calls validateColumns macro
  validateColumns(columns)

template update*(columns: varargs[untyped]): seq[string] =
  ## Template wrapper that calls validateColumns macro
  validateColumns(columns)

template where*(columns: varargs[untyped]): seq[string] =
  ## Template wrapper that calls validateColumns macro
  let t = validateColumns(columns)
  var res: seq[string]
  for v in t:
    if not v.contains(" "):
      res.add(v & " =")
    else:
      res.add(v)
  res

template insert*(columns: varargs[untyped]): seq[string] =
  ## Template wrapper that calls validateColumns macro
  let t = validateColumns(columns)
  var res: seq[string]
  for v in t:
    if v.contains("."):
      let parts = v.split(".")
      if parts.len == 2:
        res.add(parts[1])
    else:
      res.add(v)
  res


# let select = (@[
#   db(person.id),
#   db(person.uuid),
#   db(company_notes.id)
# ])
# echo select

# let select2 = validateColumns(
#   person.id,
#   person.uuid,
#   company_notes.id
# )
# echo select2

# let select3 = validateColumns(
#   "person.id AS person_id",
#   person.uuid,
#   "COUNT(company_notes.id)"
# )
# echo select3

# let select4 = validateColumns(@[
#   person.id,
#   person.uuid,
#   "COUNT(company_notes.id)"
# ])
# echo select4


# let where1 = validateColumns(
#   "person.id = 1",
# )
# echo where1

# let where2 = validateColumns(
#   "person.id = 1",
#   "person.uuid = '123'",
# )
# echo where2

