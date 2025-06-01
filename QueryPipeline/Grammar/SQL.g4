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
    | updateStatement;

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
        | getDate;
        
getDate
    : 'GETDATE()'
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

BOOL            : 'BOOL';
DATETIME        : 'DATETIME';
DECIMAL         : 'DECIMAL';
TINYINT         : 'TINYINT';
SMALLINT        : 'SMALLINT';
INT             : 'INT';
BIGINT          : 'BIGINT';

VARCHAR         : 'VARCHAR';
NVARCHAR        : 'NVARCHAR';
MAX             : 'MAX';

NOT             : 'NOT';
NULL            : 'NULL';

DESC            : 'DESC';
ASC             : 'ASC';


WILDCARD        : '*';
IDENTIFIER      : [a-zA-Z_][a-zA-Z0-9_]*;
STRING          : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER          : [0-9]+;
WS              : [ \t\r\n]+ -> skip;