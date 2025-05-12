grammar SQL;

sqlStatement : selectStatement | insertStatement | updateStatement;

selectStatement : 'SELECT' columnList 'FROM' tableName;
insertStatement : 'INSERT' 'INTO' tableName '(' columnList ')' 'VALUES' '(' valueList ')';
updateStatement : 'UPDATE' tableName 'SET' columnValueList 'WHERE' condition;

columnList : columnName (',' columnName)*;
valueList : value (',' value)*;
columnValueList : columnValue (',' columnValue)*;

columnName : ID;
value : STRING | NUMBER;
columnValue : columnName '=' value;
tableName : ID;
condition : columnName '=' value;

ID : [a-zA-Z_][a-zA-Z0-9_]*;
STRING : '\'' ( ~['\\] | '\\' . )* '\''; 
NUMBER : [0-9]+;
WS : [ \t\r\n]+ -> skip;