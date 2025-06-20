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
    | createIndexStatement
    | alterTableStatement;

//select statement
selectStatement
            :   'SELECT' (columnList | WILDCARD)
                'FROM' tableName
                ((joinStatement)*)?
                whereClause?
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

//order by
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
    : columnName dataType (NOT NULL | NULL)? primaryKey? defaultValue?
    ;

defaultValue
    : 'DEFAULT' literalValue
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
    : ('CONSTRAINT' constraintName=identifier)? 'PRIMARY' 'KEY' '(' columnList ')'
    ;

//Create Schema
createSchemaStatement
    : 'CREATE' 'SCHEMA' identifier
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
        | NULL
        | identifier;

//Create Index
createIndexStatement
        : 'CREATE' (UNIQUE)? 'INDEX' identifier 'ON' tableName '(' columnList ')'
        ;

//Join
joinStatement
    : joinType? JOIN tableName 'ON' expression
    ;

joinType
    : INNER
    | LEFT (OUTER)?
    | RIGHT (OUTER)?
    | FULL (OUTER)?
    ;

//Get Date
getDate
    : 'GETDATE()'
    ;

//New Guid
newGuid
    : 'NEWID()'
    ;
    
columnList : columnName (',' columnName)*;

//declarations for clarification
dbName: identifier;
columnName : (columnAlias)? name=identifier;

columnAlias
    : identifier '.'
    ;

tableName : (schemaName=identifier '.')? name=identifier (alias)?;

alias
    : (AS)? identifier;

//create database statement
createDbStatement: 'CREATE' 'DATABASE' identifier;

//drop database statement
dropDbStatement: 'DROP' 'DATABASE' identifier;

alterTableStatement
    : 'ALTER' 'TABLE' tableName alterTableAction;

alterTableAction
    : alterTableAddColumn
    | alterTableDropColumn
    | alterTableModifyColumn
    | alterTableRenameColumn
    ;

alterTableAddColumn
    : 'ADD' 'COLUMN' addColumn
    ;

alterTableDropColumn
    : 'DROP' 'COLUMN' columnName
    ;

alterTableModifyColumn
    : 'ALTER' 'COLUMN' columnName dataType
    ;

alterTableRenameColumn
    : 'RENAME' 'COLUMN' oldName=columnName 'TO' newName=columnName
    ;

identifier
    : ('[')? IDENTIFIER (']')?
    ;

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

//Joins
LEFT            : 'LEFT';
RIGHT           : 'RIGHT';
FULL            : 'FULL';
INNER           : 'INNER';
OUTER           : 'OUTER';
JOIN            : 'JOIN';

//Constraints
UNIQUE          : 'UNIQUE';

//Alias
AS              : 'AS';

//More Datatypes and identifiers
WILDCARD        : '*';
IDENTIFIER      : [a-zA-Z_][a-zA-Z0-9_]*;
STRING          : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER          : [0-9]+;
WS              : [ \t\r\n]+ -> skip;