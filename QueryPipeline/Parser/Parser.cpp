#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../Visitor/Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../PhysicalPlan/PhysicalPlan.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>

namespace QueryPipeline
{
    Parser::Parser() = default;

    Statements::Statement * Parser::CreateStatement(const std::any &ast){
        if (ast.type() == typeid(Statements::SelectStatement))
            return new Statements::SelectStatement(std::any_cast<Statements::SelectStatement>(ast));
        if (ast.type() == typeid(Statements::CreateDbStatement))
            return new Statements::CreateDbStatement(std::any_cast<Statements::CreateDbStatement>(ast));
        if (ast.type() == typeid(Statements::DropDbStatement))
            return new Statements::DropDbStatement(std::any_cast<Statements::DropDbStatement>(ast));
        if (ast.type() == typeid(Statements::InsertStatement))
            return new Statements::InsertStatement(std::any_cast<Statements::InsertStatement>(ast));
        if (ast.type() == typeid(Statements::InsertStatement))
            return new Statements::InsertStatement(std::any_cast<Statements::InsertStatement>(ast));
        if (ast.type() == typeid(Statements::CreateTableStatement))
            return new Statements::CreateTableStatement(std::any_cast<Statements::CreateTableStatement>(ast));

        return nullptr;
    }        


    Parser::~Parser() = default;

    void Parser::Parse(const string& query)
    {
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

        Statements::Statement* statement = Parser::CreateStatement(response);

        if (statement == nullptr)
            throw runtime_error("Failed to parse query");
        
        statement->Validate();

        LogicalPlan* logicalPlan = statement->ToLogical();
        
        if (logicalPlan == nullptr)
            return;

        PhysicalPlan::PhysicalOperator* physicalPlan = logicalPlan->ToPhysical();
        const auto result = physicalPlan->Execute();

        for (const auto& row: result.rows) {
            row.PrintRow();
        }

        
        delete statement;
        delete logicalPlan;
        delete physicalPlan;
    }
}