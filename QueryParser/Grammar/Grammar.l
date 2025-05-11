%option noyywrap

%{
#include "parser.hpp"
#define YY_DECL int yylex(yy::parser::semantic_type *yyval)
%}

%%


%%