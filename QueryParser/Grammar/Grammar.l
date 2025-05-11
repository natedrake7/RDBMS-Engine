%option noyywrap

%{
#include <cstdio> 
#include <cstring>
#include "parser.hpp"
#define YY_DECL int yylex(yy::parser::semantic_type *yyval)

extern FILE* yyin;  // Declares the input stream
extern int yylex(yy::parser::semantic_type *yyval);
void set_yyin(FILE* file);

%}

DIGIT      [0-9]
ID_START   [a-zA-Z_]
ID_BODY    [a-zA-Z0-9_]
IDENTIFIER {ID_START}{ID_BODY}*
NUMBER     {DIGIT}+
STRING     \'[^\']*\'

%%


"SELECT"        { return yy::parser::token::SELECT; }
"FROM"          { return yy::parser::token::FROM; }
"WHERE"         { return yy::parser::token::WHERE; }
"INSERT"        { return yy::parser::token::INSERT; }
"INTO"          { return yy::parser::token::INTO; }
"VALUES"        { return yy::parser::token::VALUES; }
"CREATE"        { return yy::parser::token::CREATE; }
"TABLE"         { return yy::parser::token::TABLE; }

{IDENTIFIER}    { yyval->str = strdup(yytext); return yy::parser::token::IDENTIFIER; }
{NUMBER}        { yyval->intval = std::stoi(yytext); return yy::parser::token::NUMBER; }
{STRING}        { yyval->str = strdup(yytext + 1); yyval->str[strlen(yyval->str) - 1] = '\0'; return yy::parser::token::STRING; }

","             { return yy::parser::token::COMMA; }
";"             { return yy::parser::token::SEMICOLON; }
"="             { return yy::parser::token::EQ; }

[ \t\n\r]+      { /* skip whitespace */ }

.               { return *yytext; }  // fallback: return unknown character

%%

void set_yyin(FILE* file){
    yyin = file;
}