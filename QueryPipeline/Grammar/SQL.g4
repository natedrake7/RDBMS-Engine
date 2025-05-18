grammar SQL;

sqlStatement : selectStatement | createDbStatement | dropDbStatement;

selectStatement : 'SELECT' columnList 'FROM' tableName whereClause?;

whereClause: 'WHERE' expression ('AND' expression)*
            ;
expression
        : columnName op=(EQUAL | NOTEQUAL | LESSTHAN | GREATERTHAN | LESS | GREATER) literalValue
        ;

literalValue
        : STRING 
        | NUMBER;
        
columnList : columnName (',' columnName)*;

dbName: IDENTIFIER;
columnName : IDENTIFIER;
tableName : IDENTIFIER;

createDbStatement: 'CREATE' 'DATABASE' IDENTIFIER;

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