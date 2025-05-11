%option noyywrap

%{
#include <cstdio> 
#include "parser.hpp"
#define YY_DECL int yylex(yy::parser::semantic_type *yyval)

extern FILE* yyin;  // Declares the input stream
extern int yylex(yy::parser::semantic_type *yyval);
void set_yyin(FILE* file);

%}

%%

[1-9][0-9]* { printf("Number: %s", yytext); }

%%

void set_yyin(FILE* file){
    yyin = file;
}