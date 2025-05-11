%language "C++"
%start root

%{

#include "parser.hpp"
#include <string>

extern int yylex(yy::parser::semantic_type *yyval);
%}

%union {
    int intval;
    char* str;
}

// Declare tokens here
%token SELECT FROM WHERE INSERT INTO VALUES CREATE TABLE

%token <str> IDENTIFIER STRING
%token <intval> NUMBER

%token COMMA
%token SEMICOLON
%token EQ

%type <str> column_name

%%


root
  : SELECT column_list_statement FROM IDENTIFIER
      { std::cout << "Parsed SELECT statement\n"; }
  ;
  
column_list_statement
  : column_list
  ;

column_list
  : column_name                          { std::cout << "Column: " << $1 << std::endl; }
  | column_list COMMA column_name         { std::cout << "Column: " << $3 << std::endl; }
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
