# RDBMS Engine (C++23)

An ongoing RDBMS Engine written in C++.

## Purpose

The purpose of this project is to create an SQL database in modern C++ (C++23).
This project started as a learning exercise to understand how databases work under the hood.
The goal is to create a fully functional SQL database engine that supports a wide range of SQL features
and is efficient and reliable.
The project is open-source and welcomes contributions from the community.

## Status

The Engine is still under development.

## Currently Supported Statements

1. **SELECT**
2. **UPDATE**
3. **INSERT**
4. **DELETE**
5. **CREATE TABLE**
6. **ALTER TABLE**
7. **CREATE INDEX**

## SELECT Query

The syntax for select is as follows:

  SELECT columns 
  
  FROM [table](#table)

  [JoinClause](#join-clause)

  WHERE [Where Clause](#where-clause)

  ORDER BY [Order By Clause](#order-by-clause)

## Where Clause

The where clause is optional and can be used to filter the results of the select statement.
It utilizes the below [Expression](#expression) functionality to allow complex filterings.
It only allows Logical and Binary type of expressions as top level expressions. Their respective children
can be of any expression type.

### Where Clause Best Practices

Correct usage of the `WHERE` clause can greatly impact the runtime of a query.

- If the where clause is not provided, all rows from the table are returned.
- If an **index** can be used in the clause, the engine defaults to **Index Seek** or **Index Scan**, which are much faster than a **Table Scan**, as the **B+ Tree implementation** is leveraged.
- Whenever possible, avoid using `OR` expressions or deeply nested expressions, as they can prevent the engine from efficiently using indexes and may lead to slower query execution.

## Order By Clause


## Join Clause
The syntax for join is as follows:

  JOIN table ON expression

where: table is a table from the selected database,


## UPDATE Query

The syntax for update is as follows:

  UPDATE table
  SET updateConditions
  WHERE conditions

where: table is a table from the selected database,
update conditions are condtions of type column = some value,
conditions like the select statement.

## INSERT Query

INSERT INTO 
  table (listOfColumns) 
VALUES (listOfValues)

## DELETE Query
  DELETE FROM table 
  WHERE conditions

## Table

The table property in the above queries can have the following syntax:

table where the schema [dbo] is selected by default,

schema.table where the table is selected from the selected schema,

schema.table AS alias where the table is selected from the selected schema and an alias is given to the table.

optional brackets can be used on all identifiers, e.g. [schema].[table] AS alias.

## Expressions
The expressions property above is a list of [Expression](#expression) separated by commas.

## Expression

An expression can be one of the following:
1. [Column Expression](#column-expression)
2. [Literal Expression](#literal-expression)
3. [Function Expression](#function-expression)
4. [Binary Expression](#binary-expression)
5. [Logical Expression](#logical-expression)

# Column Expression
A column expression is the usage of an identifier that represents a column in a table (e.g ID).

# Literal Expression
A literal expression is any literal value (e.g 5, 'Hello', 3.14).

# Function Expression
A function expression is the usage of a function (e.g COUNT(ID), SUM(Price))

## Valid Functions

### Aggregate Functions
| Function | Description |
|----------|-------------|
| COUNT(expr) | Returns the number of rows (or non-NULL values if an expression is provided) |
| SUM(expr)   | Returns the sum of all non-NULL values in the expression |
| AVG(expr)   | Returns the average value of the expression |
| MIN(expr)   | Returns the minimum value in the expression |
| MAX(expr)   | Returns the maximum value in the expression |

### String Functions
| Function | Description |
|----------|-------------|
| LENGTH(string)         | Returns the number of characters in the string |
| CONCAT(a, b, …)       | Concatenates multiple strings |
| ASCII(char)            | Returns the ASCII code of the first character |
| CHAR(code)             | Returns the character for an ASCII code |
| CHARINDEX(substr, str) | Returns the 1-based position of `substr` in `str` |
| LOWER(string)          | Converts string to lowercase |
| UPPER(string)          | Converts string to uppercase |
| TRIM(string)           | Removes leading and trailing spaces |
| TRIMLEFT(string)       | Removes leading spaces |
| TRIMRIGHT(string)      | Removes trailing spaces |
| REPLACE(str, from, to) | Replaces all occurrences of `from` with `to` |
| SUBSTR(str, start, length) | Extracts a substring |
| LEFT(str, n)           | Returns the leftmost `n` characters |
| RIGHT(str, n)          | Returns the rightmost `n` characters |
| REVERSE(str)           | Returns the string reversed |
| SPACE(n)               | Returns a string of `n` spaces |

### Date / Utility Functions
| Function | Description |
|----------|-------------|
| GETDATE() | Returns the current date/time |
| NEWID()   | Returns a new GUID/UUID |


# Binary Expression
A binary expression is an expression that has two operands and an operator (e.g a + b, a - b, a * b, a / b).

## Operator Categories

| Category                | Operators         |
|-------------------------|-----------------|
| Additive Operators      | `+`, `-`         |
| Multiplicative Operators| `*`, `/`, `%`    |
| Comparison Operators    | `=`, `<>`, `<`, `>`, `<=`, `>=` |

and their precedence is as follows (from highest to lowest):

1) Multiplicative Operators
2) Additive Operators
3) Comparison Operators

Each Binary Expression is depicted as a binary tree where each node has two children (left and right).
Each child can be of either expression type so nesting can be done.

# Logical Expression
A logical expression is an expression that has two operands and a logical operator (e.g a AND b, a OR b).

Valid operators are:
1) AND,
2) OR,
3) NOT.

Their precedence is as follows (from highest to lowest):
1) NOT
2) AND
3) OR

Each Logical Expression is depicted as a binary tree where each node has two children (left and right).
Each child can be of either expression type so nesting can be done.

# Engine Architecture

## Storage Engine

The engine utilizes 2 main entities to handle data storage:

| Name                 | Operation     |
|----------------------|---------------|
| File Manager         | Handles Files |
| Storage Manager      | Handles Pages |

### File Manager

The File Manager is responsible for handling files on the disk.
It provides an abstraction over the file system and allows for easy file operations such as reading, writing
and deleting files.

Additionally it implemented and LRU Cache for file descriptors so when a new file is needed to be opened but the 
maximum number of open files is reached, the least recently used file descriptor is closed to make room for the new one.

### Storage Manager

The Storage Manager is responsible for handling pages in memory.
It provides an abstraction over the memory and allows for easy page operations such as reading, writing
and deleting pages.

Additionally it implemented and LRU Cache for pages so when a new page is needed to be loaded but the
maximum number of pages in memory is reached, the least recently used page is evicted to make
room for a new one.

Both the File Manager and Storage Manager are designed to be efficient and reliable, ensuring that data is stored
safely and can be accessed quickly when needed.

## Database Engine

## Query Pipeline


## Server

