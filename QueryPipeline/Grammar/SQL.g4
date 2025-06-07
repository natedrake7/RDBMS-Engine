grammar SQL;

options {
  caseInsensitive = true;
}

sqlStatement 
    : selectStatement 
    | createDbStatement 
    | dropDbStatement 
    | insertStatement 
    | createTableStatement
    | createSchemaStatement
    | deleteStatement
    | updateStatement
    | createIndexStatement;

//select statement
selectStatement
            : 'SELECT' (columnList | WILDCARD)
              'FROM' tableName whereClause?
               orderByStatement?
            ;

whereClause
    : 'WHERE' expression
    ;

expression
    : orExpression
    ;

orExpression
    : andExpression ('OR' andExpression)*
    ;

andExpression
    : predicate ('AND' predicate)*
    ;

predicate
    : '(' expression ')'
    | columnName op=(EQUAL | NOTEQUAL | LESSTHAN | GREATERTHAN | LESS | GREATER) literalValue
    ;

orderByStatement
    : 'ORDER' 'BY' columnList order=(DESC | ASC)?
    ;

//insert statement
insertStatement
        : 'INSERT' 'INTO' tableName '(' columnList ')' 'VALUES' '(' literalValueList ')'
        ;

deleteStatement
        : 'DELETE' 'FROM' tableName whereClause?
        ;

//create table statement
createTableStatement: 'CREATE' 'TABLE' tableName '(' addColumn (',' addColumn)* ( ',' primaryKeyConstraint)? ')';

addColumn
    : columnName dataType (NOT NULL | NULL)? primaryKey?
    ;

dataType
    : BOOL
    | TINYINT
    | SMALLINT
    | INT
    | BIGINT
    | varcharType
    | nvarcharType
    | decimalType
    | DATETIME
    | GUID
    ;

varcharType
    : 'VARCHAR' '(' (num=NUMBER | max=MAX) ')'
    ;

nvarcharType
    : NVARCHAR '(' (num=NUMBER | max=MAX) ')'
    ;

decimalType
    : DECIMAL '(' (beforePoint=NUMBER) ',' (afterPoint=NUMBER) ')'
    ;

primaryKey
    : 'PRIMARY' 'KEY' autoIncrementKey?
    ;

autoIncrementKey
    : 'IDENTITY' '('(seed=NUMBER) ',' (increment=NUMBER)')'
    ;
    
primaryKeyConstraint
    : ('CONSTRAINT' constraintName=IDENTIFIER)? 'PRIMARY' 'KEY' '(' columnList ')'
    ;

//Create Schema
createSchemaStatement
    : 'CREATE' 'SCHEMA' IDENTIFIER
    ;

//Update statement
updateStatement
    : 'UPDATE' tableName 'SET' updateColumnsList whereClause?
    ;

updateColumnsList
    :  updateColumn (',' updateColumn)*
    ;

updateColumn
    : columnName EQUAL literalValue
    ;

//helpers
literalValueList
    : literalValue (',' literalValue)*;

literalValue
        : STRING 
        | NUMBER
        | getDate
        | newGuid
        | NULL;

createIndexStatement
        : 'CREATE' (UNIQUE)? 'INDEX' IDENTIFIER 'ON' tableName '(' columnList ')'
        ;
        
getDate
    : 'GETDATE()'
    ;

newGuid
    : 'NEWID()'
    ;
    
columnList : columnName (',' columnName)*;

//declarations for clarification
dbName: IDENTIFIER;
columnName : IDENTIFIER;
tableName : (schemaName=IDENTIFIER '.')? name=IDENTIFIER;

//create database statement
createDbStatement: 'CREATE' 'DATABASE' IDENTIFIER;

//drop database statement
dropDbStatement: 'DROP' 'DATABASE' IDENTIFIER;

//Comparison Operators
NOTEQUAL        : '!=' | '<>';
LESS            : '<=';
GREATER         : '>=';
LESSTHAN        : '<';
GREATERTHAN     : '>';
EQUAL           : '=';

//DataTypes
GUID            : 'GUID';
BOOL            : 'BOOL';
DATETIME        : 'DATETIME';
DECIMAL         : 'DECIMAL';
TINYINT         : 'TINYINT';
SMALLINT        : 'SMALLINT';
INT             : 'INT';
BIGINT          : 'BIGINT';

//VARCHAR Types
VARCHAR         : 'VARCHAR';
NVARCHAR        : 'NVARCHAR';
MAX             : 'MAX';

//Nullable
NOT             : 'NOT';
NULL            : 'NULL';

//Ordering
DESC            : 'DESC';
ASC             : 'ASC';

//Constraints
UNIQUE          : 'UNIQUE';

//More Datatypes and identifiers
WILDCARD        : '*';
IDENTIFIER      : [a-zA-Z_][a-zA-Z0-9_]*;
STRING          : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER          : [0-9]+;
WS              : [ \t\r\n]+ -> skip;