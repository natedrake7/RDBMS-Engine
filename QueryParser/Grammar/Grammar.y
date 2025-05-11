%language "C++"
%start root

%{

#include "parser.hpp"

extern int yylex(yy::parser::semantic_type *yyval);
%}


%%


root : ;


%%

namespace yy{
    void parser::error(const std::string& msg){
        std::cerr << msg << std::endl;
    }
}
