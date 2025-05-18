grammar SQL;

sqlStatement : selectStatement | createDbStatement | dropDbStatement | insertStatement;

//select statement
selectStatement : 'SELECT' columnList 'FROM' tableName whereClause?;

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
    


//insert statement
insertStatement: 'INSERT' 'INTO' tableName '(' columnList ')' 'VALUES' '(' literalValueList ')'
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
tableName : IDENTIFIER;


//create database statement
createDbStatement: 'CREATE' 'DATABASE' IDENTIFIER;

//drop database statement
dropDbStatement: 'DROP' 'DATABASE' IDENTIFIER;

IDENTIFIER      : [a-zA-Z_][a-zA-Z0-9_]*;
STRING          : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER          : [0-9]+;
WS              : [ \t\r\n]+ -> skip;

//Comparison Operators
NOTEQUAL        : '!=' | '<>';
LESS            : '<=';
GREATER         : '>=';
LESSTHAN        : '<';
GREATERTHAN     : '>';
EQUAL           : '=';