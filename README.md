# RDBMS Engine (C++23)

An ongoing RDBMS Engine written in C++.

## Purpose

The purpose of this project is to create an SQL database in modern C++ (CXX26).

## Status

The Engine is still under development but supports many features.

## Currently Supported Queries

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

  WHERE conditions

  ORDER BY columns orderType

where: columns are columns from the table,
table is a table from the selected database,
conditions can be predicates OR, AND or even combinations of them,
orderType is either "DESC" or "ASC".

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

## Expression


## Storage Architecture

This project follows the standards of SQL Server in how it utilizes disk I/Os. Furthermore:

- Data is segmented into **pages**
- A **storage manager** handles allocation, deletion, and saving of pages
- An **LRU cache** evicts unused pages when cache capacity is reached and a new page must be loaded from disk
