grammar SQL;

sqlStatement : selectStatement | createDbStatement | dropDbStatement;

selectStatement : 'SELECT' columnList 'FROM' tableName;

createDbStatement: 'CREATE' 'DATABASE' IDENTIFIER;

dropDbStatement: 'DROP' 'DATABASE' IDENTIFIER;

columnList : columnName (',' columnName)*;

dbName: IDENTIFIER;
columnName : IDENTIFIER;
tableName : IDENTIFIER;

IDENTIFIER : [a-zA-Z_][a-zA-Z0-9_]*;
STRING : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER : [0-9]+;
WS : [ \t\r\n]+ -> skip;