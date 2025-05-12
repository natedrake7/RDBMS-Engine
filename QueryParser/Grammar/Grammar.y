%language "C++"
%start root

%{

#include "parser.hpp"

extern int yylex(yy::parser::semantic_type *yyval);
class Expression;
class BinaryExpression;
class IdentifierExpression;
class StringExpression;
class NumberExpression;

%}

%code requires {
    #include "QueryParser/Expression.h"
    #include <string>
    #include <memory>
}

%union {
    int intval;
    char* str;
    Expression* expression;
    BinaryExpression* binaryExpression;
    IdentifierExpression* identifierExpression;
    StringExpression* stringExpression;
    NumberExpression* numberExpression;
}

%parse-param { std::string* ast }

%type <str> column_name
%type <expression> expression

%left OR
%left AND
%left '=' '<' '>'

// Declare tokens here
%token SELECT FROM WHERE INSERT INTO VALUES CREATE TABLE

%token <str> IDENTIFIER STRING
%token <intval> NUMBER

%token COMMA
%token SEMICOLON

%token AND OR
%token EQUAL NOTEQUAL LESS GREATER LESSEQUAL GREATEREQUAL
%token PLUS MINUS MULTIPLY DIVIDE
%token LEFT_PARENTHESIS RIGHT_PARENTHESIS

%%


root
  : select_statement
  ;
  
select_statement
    : SELECT column_list_statement FROM IDENTIFIER WHERE expression opt_semicolon { *ast = "Parsed Query"; }
    ;
    
expression
    : expression PLUS expression                         { $$ = new BinaryExpression($1, "+", $3); }
    | expression MINUS expression                        { $$ = new BinaryExpression($1, "-", $3); }
    | expression MULTIPLY expression                     { $$ = new BinaryExpression($1, "*", $3); }
    | expression DIVIDE expression                       { $$ = new BinaryExpression($1, "/", $3); }
    | expression EQUAL expression                        { $$ = new BinaryExpression($1, "=", $3); }
    | expression NOTEQUAL expression                     { $$ = new BinaryExpression($1, "<>", $3); }
    | expression LESS expression                         { $$ = new BinaryExpression($1, "<", $3); }
    | expression GREATER expression                      { $$ = new BinaryExpression($1, ">", $3); }
    | expression LESSEQUAL expression                    { $$ = new BinaryExpression($1, "<=", $3); }
    | expression GREATEREQUAL expression                 { $$ = new BinaryExpression($1, ">=", $3); }
    | expression AND expression                          { $$ = new BinaryExpression($1, "AND", $3); }
    | expression OR expression                           { $$ = new BinaryExpression($1, "OR", $3); }
    | LEFT_PARENTHESIS expression RIGHT_PARENTHESIS      { $$ = $2; }
    | IDENTIFIER                                         { $$ = new IdentifierExpression($1); }
    | NUMBER                                             { $$ = new NumberExpression(std::to_string($1)); }   
    | STRING                                             { $$ = new StringExpression($1); }
    ;
  
opt_semicolon
  : SEMICOLON
  | /* empty */
  ;
  
column_list_statement
  : column_name                          { std::cout << "Column: " << $1 << std::endl; }
  | column_list_statement COMMA column_name         { std::cout << "Column: " << $3 << std::endl; }
  ;

column_name
  : IDENTIFIER                          { $$ = $1; }
  ;

%%

namespace yy{
    void parser::error(const std::string& msg){
        std::cerr << msg << std::endl;
    }
}