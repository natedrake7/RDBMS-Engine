grammar SQL;

sqlStatement : selectStatement | createDbStatement;

selectStatement : 'SELECT' columnList 'FROM' tableName;

createDbStatement: 'CREATE' 'DATABASE' dbName;

columnList : columnName (',' columnName)*;

dbName: ID;
columnName : ID;
tableName : ID;

ID : [a-zA-Z_][a-zA-Z0-9_]*;
STRING : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER : [0-9]+;
WS : [ \t\r\n]+ -> skip;