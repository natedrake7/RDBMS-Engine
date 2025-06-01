# RDBMS Engine (C++26)

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

## SELECT Query

The syntax for select is as follows:

  SELECT columns FROM table
  WHERE conditions
  ORDER BY columns orderType

where: columns are columns from the table,
table is a table from the selected database,
conditions can be predicates OR, AND or even combinations of them,
orderType is either "DESC" or "ASC".

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

## Storage Architecture

This project follows the standards of SQL Server in how it utilizes disk I/Os. Furthermore:

- Data is segmented into **pages**
- A **storage manager** handles allocation, deletion, and saving of pages
- An **LRU cache** evicts unused pages when cache capacity is reached and a new page must be loaded from disk
