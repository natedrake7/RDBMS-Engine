#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../Visitor/Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../PhysicalPlan/PhysicalPlan.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>
#include <variant>

namespace QueryPipeline
{
    Parser::Parser() = default;

    using StatementVariant = variant<
        Statements::SelectStatement,
        Statements::CreateDbStatement,
        Statements::DropDbStatement,
        Statements::InsertStatement,
        Statements::CreateTableStatement
    >;

    Statements::Statement* Parser::CreateStatement(const std::any &ast, const std::string& dbName){
        function<Statements::Statement *(const any &)> handler;

        if (!handlers.TryGetValue(ast.type(), handler))
            return nullptr;

        Statements::Statement* statement = handler(ast);
        statement->dbName = dbName;
        
        return statement;
    }

    Parser::~Parser() = default;

    void Parser::Parse(const string& query, const std::string& dbName){
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

        Statements::Statement* statement = Parser::CreateStatement(response, dbName);

        if (statement == nullptr)
            throw runtime_error("Failed to parse query");
        
        statement->Validate();

        LogicalPlan* logicalPlan = statement->ToLogical();
        
        if (logicalPlan == nullptr)
            return;

        PhysicalPlan::PhysicalOperator* physicalPlan = logicalPlan->ToPhysical();
        const auto* result = physicalPlan->Execute();

        if (result != nullptr) {
            for (const auto& row: result->rows) {
                row.PrintRow();
            }
        }

        delete result;
        delete statement;
        delete logicalPlan;
        delete physicalPlan;
    }
}