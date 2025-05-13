grammar SQL;

sqlStatement : selectStatement ;

selectStatement : 'SELECT' columnList 'FROM' tableName;

columnList : columnName (',' columnName)*;

columnName : ID;
tableName : ID;

ID : [a-zA-Z_][a-zA-Z0-9_]*;
STRING : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER : [0-9]+;
WS : [ \t\r\n]+ -> skip;