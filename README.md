# sqlquery

An opinionated SQL query builder for Nim with compile-time schema validation. This library generates type-safe SQL queries while automatically handling common patterns like soft deletes.

> **Note**: You can use the library in two ways for executing queries:
> - **With a normal `DbConn`** (e.g. from `std/db_sqlite`, `std/db_postgres`): pass the connection as a named argument to `selectRows(db = ..., ...)`, `selectRow(db = ..., ...)`, `updateValues(db = ..., ...)`, and the other execution macros. **Waterpark is not required** — all of these macros accept `db: DbConn` as the first argument.
> - **With [waterpark](https://github.com/guzba/waterpark)** for PostgreSQL connection pooling: use the same macros without a connection argument (e.g. `selectRows(table = "...", ...)`); they will use the global `pg` pool.
>
> Query generation (`selectQuery`, `insertQuery`, etc.) **does not** require waterpark and works with any database flow.

## Overview

`sqlquery` is designed for developers who want:
- **Type safety**: Compile-time validation of table and column names
- **Automatic soft-delete handling**: Tables with `is_deleted` fields are automatically filtered
- **Parameterized queries**: Built-in SQL injection protection
- **Opinionated defaults**: Enforces best practices and common patterns

This is an **opinionated** builder - it makes decisions for you to ensure consistency and safety. If you need complete flexibility, this may not be the right tool.

## Installation

Add to your `nimble` file:

```nim
requires "sqlquery >= 0.1.0"
```

Or install directly:

```bash
nimble install sqlquery
```

## Quick Start

### 1. Define Your Schema

Create SQL schema files that the library will parse:

```sql
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  status VARCHAR(50),
  is_deleted TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS projects (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  user_id INTEGER,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);
```

### 2. Configure Schema Path

Set the schema path when compiling:

```bash
nim c -d:SqlSchemaPath="path/to/your/schema" your_app.nim
```

Or in your `.nimble` file:

```nim
switch("define", "SqlSchemaPath=resources/sql/schema")
```

### 3. Basic Query Examples

#### SELECT Query

```nim
import sqlquery

# Simple select with where clause and joins
let query = selectQuery(
  table = "projects",
  select = @[
    "projects.name",
    "projects.creation",
    "users.name"
  ],
  joins = @[
    ("users", LEFTJOIN, @[("users.id", "=", "projects.user_id")])
  ],
  where = @[
    ("projects.status", "=", "active")
  ],
  order = @[("projects.creation", DESC)]
)

echo query.sql
# SELECT projects.name, projects.creation, users.name
# FROM projects
# LEFT JOIN users ON users.id = projects.user_id
# AND users.is_deleted IS NULL
# WHERE projects.status = ?
# AND projects.is_deleted IS NULL
# ORDER BY projects.creation DESC

echo query.params
# @["active"]
```

#### INSERT Query

```nim
let query = insertQuery(
  table = "users",
  data = @[
    ("name", "John Doe"),
    ("email", "john@example.com"),
    ("status", "active")
  ]
)

echo query.sql
# INSERT INTO users (name, email, status) VALUES (?, ?, ?)

echo query.params
# @["John Doe", "john@example.com", "active"]
```

### Executing Queries and Accessing Results

The library provides convenient macros for executing queries and working with results. You can use a **normal `DbConn`** (no waterpark) by passing it as the first argument:

#### Using a normal DbConn (no waterpark)

Waterpark is not required. All execution macros (`selectRows`, `selectRow`, `updateValues`, etc.) accept a `DbConn` as the first argument:

```nim
import std/db_sqlite  # or std/db_postgres
import sqlquery

let db = open("mydb.sqlite", "", "", "")  # or open a Postgres connection

# Pass db as first argument; works with any DbConn
let data = selectRows(db,
  table = "company_daily_engagement",
  select = @["company_id"],
  where = @[
    ("company_id", "=", companyId),
    ("epoch_day", ">=", $epoch14dAgo),
    ("sql:>status IN ('warning', 'critical')", "", ""),
  ]
)

for row in loopRows(data):
  echo row.get("company_id")
db.close()
```

#### Using `selectRows` with waterpark (optional)

```nim
# Initialize your database connection pool (waterpark library)
pg = newPostgresPool("your_connection_string")

# Execute query and get all rows (no db argument – uses pg pool)
let rows = selectRows(
  table = "users",
  select = @["users.id", "users.name", "users.email"],
  where = @[("users.status", "=", "active")],
  order = @[("users.name", ASC)]
)

# Loop through rows using loopRows()
for row in loopRows(rows):
  # Access fields using get()
  echo "ID: ", row.get("users.id")
  echo "Name: ", row.get("name") # or just the field
  echo "Email: ", row.get("users.email")
```

#### Working with Single Rows

```nim
# For a single row, use selectRow
let row = selectRow(
  table = "users",
  select = @["users.id", "users.name", "users.email"],
  where = @[("users.id", "=", "123")]
)

# Access fields directly
echo row.get("users.name")   # "John Doe"
echo row.get("users.email")   # "john@example.com"
```

#### Field Access with `get()`

The `get()` method supports multiple ways to access fields:

```nim
let row = selectRow(
  table = "users",
  select = @["users.id AS user_id", "users.name", "users.email"],
  where = @[("users.id", "=", "123")]
)

# Access by full table.field name
echo row.get("users.name")    # "John Doe"

# Access by alias
echo row.get("user_id")        # "123"

# Access by field name only (if unambiguous)
echo row.get("name")          # "John Doe"
```

## Advanced Features

### Custom SQL Expressions

> **⚠️ Security Warning:** The `sql:>` prefix bypasses all validation and can introduce SQL injection vulnerabilities. Only use with trusted, compile-time constants. See the [Security](#security) section for details.

When you need to escape the validation system, use the `sql:>` prefix:

```nim
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[
    ("sql:>users.status = 'active' OR users.creation > NOW() - INTERVAL '1 day'", "", ""),
    ("users.email", "IS NOT", "NULL"),
    ("users.id", "= ANY(?::int[])", "1,2,3,4"),
    ("users.status", "<> ALL(?::text[])", "banned,archived")  # NOT IN: value not in array
  ]
)

# = ANY(?::type[]) means "value IN array"; <> ALL(?::type[]) means "value NOT IN array".
# The sql:> prefix bypasses validation and wraps the expression in parentheses
# ⚠️ Only use with compile-time constants, never with user input!
```

### Array Operations

```nim
# Using PostgreSQL array functions
let query = updateQuery(
  table = "checklists",
  data = @[
    ("imported_uuids = array_append(COALESCE(imported_uuids, '{}'), ?)", "1234-1234-1234")
  ],
  where = @[("id", "=", "123")]
)
```

### Complex WHERE Conditions

```nim
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[
    ("users.status", "IN", "('active', 'pending')"),
    ("users.creation", "BETWEEN", "2024-01-01 AND 2024-12-31"),
    ("length(users.name)", ">", "5")  # SQL functions are supported
  ]
)
```

## Security

### SQL Injection Protection

By default, `sqlquery` provides strong SQL injection protection through **parameterized queries**. All user-provided values are automatically parameterized using `?` placeholders, which are safely escaped by the underlying database driver.

** Safe - Parameterized Queries (Recommended)**

```nim
# All values are automatically parameterized - SAFE even with user input
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[
    ("users.email", "=", userEmail),  # SAFE - automatically parameterized
    ("users.status", "=", userStatus)  # SAFE - automatically parameterized
  ]
)
# Generated SQL: WHERE users.email = ? AND users.status = ?
# Parameters: @[userEmail, userStatus]
```

** Safe - Array Parameters**

Use `= ANY(?::type[])` for "value IN array" and `<> ALL(?::type[])` for "value NOT IN array". Both are safely parameterized.

```nim
# IN array: = ANY(?::type[])
let query = selectQuery(
  table = "actions",
  select = @["actions.id"],
  where = @[("actions.project_id", "= ANY(?::int[])", "123,456")]
)
# Generated SQL: WHERE actions.project_id = ANY(?::int[])
# Parameters: @["{123,456}"]  # SAFE - parameterized, not concatenated

# NOT IN array: <> ALL(?::type[])
let query2 = selectQuery(
  table = "actions",
  select = @["actions.id"],
  where = @[("actions.status", "<> ALL(?::text[])", "cancelled,archived")]
)
# Generated SQL: WHERE actions.status <> ALL(?::text[])
# Parameters: @["{cancelled,archived}"]  # SAFE - parameterized
```

### Escape Hatches - Use with Extreme Caution

The library provides escape hatches for advanced use cases. **These bypass validation and can introduce SQL injection vulnerabilities if misused.** Only use these with trusted, compile-time constants or carefully validated input.

#### ⚠️ `sql:>` Prefix - Bypasses All Validation

The `sql:>` prefix allows you to inject raw SQL that bypasses all schema validation. **This is dangerous if user input is used.**

**❌ UNSAFE - User Input in sql:>**

```nim
# NEVER do this with user input!
let userInput = getUserInput()  # Could be: "1' OR '1'='1"
let query = selectQuery(
  table = "users",
  select = @["users.id"],
  where = @[("sql:>users.id = " & userInput, "", "")]  # ❌ SQL INJECTION RISK!
)
```

** SAFE - Compile-time Constants Only**

```nim
# Only use sql:> with trusted, compile-time constants
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[
    ("sql:>users.status = 'active' OR users.creation > NOW() - INTERVAL '1 day'", "", ""),
    ("users.email", "IS NOT", "NULL")
  ]
)
# SAFE - sql:> expression is a compile-time constant
```


#### ⚠️ `whereString` Parameter - Raw SQL Injection

The `whereString` parameter in runtime functions allows raw SQL to be injected directly into the query. **This is extremely dangerous if user input is used.**

**❌ UNSAFE - User Input in whereString**

```nim
# NEVER do this with user input!
let userInput = getUserInput()  # Could be: "1' OR '1'='1"
let query = selectQueryRuntime(
  table = "users",
  select = @["users.id"],
  where = @[("users.id", "=", "123")],
  whereString = (where: "OR users.email = '" & userInput & "'", params: @[])  # ❌ SQL INJECTION!
)
```

** SAFE - Parameterized whereString**

```nim
# Use ? placeholders and pass values in params
let query = selectQueryRuntime(
  table = "users",
  select = @["users.id"],
  where = @[("users.id", "=", "123")],
  whereString = (where: "OR users.email = ?", params: @[userEmail])  # ✅ SAFE - parameterized
)
```

** SAFE - Compile-time Constants Only**

```nim
# Only use whereString with trusted, compile-time constants
let query = selectQueryRuntime(
  table = "users",
  select = @["users.id"],
  where = @[("users.id", "=", "123")],
  whereString = (where: "AND users.creation > NOW() - INTERVAL '7 days'", params: @[])  # ✅ SAFE
)
```

## Step-by-Step: Building Your First Query

### Step 1: Import the Library

```nim
import sqlquery
```

### Step 2: Define Your Query Structure

Start with the table name and fields you want to select:

```nim
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"]
)
```

### Step 3: Add Filtering Conditions

Add WHERE clauses to filter your data:

```nim
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[
    ("users.status", "=", "active")
  ]
)
```

### Step 4: Add Sorting and Limits

Control the order and quantity of results:

```nim
let query = selectQuery(
  table = "users",
  select = @["users.id", "users.name"],
  where = @[("users.status", "=", "active")],
  order = @[("users.name", ASC)],
  limit = 10,
  offset = 0
)
```

### Step 5: Execute with Your Database Library

Use the generated SQL and parameters with your PostgreSQL connection:

```nim
let result = await pg.query(query.sql, query.params)
```

## Runtime vs Compile-time Queries

The library provides two versions of each query function:

- **Compile-time** (`selectQuery`, `insertQuery`, etc.): Validates against schema at compile time
- **Runtime** (`selectQueryRuntime`, `insertQueryRuntime`, etc.): Skips compile-time validation, useful for dynamic queries

Use runtime versions when you need to build queries dynamically or when schema validation isn't possible at compile time.

## Configuration

### Schema Path

Set the directory containing your SQL schema files:

```bash
-d:SqlSchemaPath="path/to/schema"
```

### Soft Delete Marker

Customize the soft-delete field name (default: `is_deleted`):

```bash
-d:SqlSchemaSoftDeleteMarker="deleted_at"
```

## Contributing

Contributions are welcome! Please ensure your changes maintain the opinionated nature of the library while adding value.

## License

MIT License - see LICENSE file for details.

## Author

Thomas T. Jarløv (TTJ) - ttj@ttj.dk
