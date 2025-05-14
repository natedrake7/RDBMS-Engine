#include "Parser.h"
#include <stdexcept>
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../Visitor.h"
#include "../../Database/Database.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>

namespace QueryParser 
{
    Parser::Parser() = default;

    Parser::~Parser() = default;

    void Parser::Parse(const DatabaseEngine::Database& db)
    {
        std::string query = "SELECT user, test FROM table";

        // Create an ANTLR input stream from the file
        antlr4::ANTLRInputStream input(query);

        // Create a lexer for the input stream
        SQLLexer lexer(&input);

        // Create a token stream for the lexer
        antlr4::CommonTokenStream tokens(&lexer);

        // Create the parser, passing the token stream
        SQLParser parser(&tokens);


        // Start parsing, typically using the start rule of the grammar
        SQLParser::SqlStatementContext *tree = parser.sqlStatement();

        SQLVisitorImplementation visitor;
        const auto response = visitor.visit(tree);

        if (response.type() == typeid(SelectStatement)) {
            const auto selectStatement = std::any_cast<SelectStatement>(response);
            
            const auto table = db.OpenTable(selectStatement.table);
        }
        else if (response.type() == typeid(CreateDbStatement)) {
            
        }


        throw invalid_argument("stop debug");
    }
}