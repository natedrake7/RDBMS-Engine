#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../Validator/Validator.h"
#include "../PhysicalPlan/PhysicalPlan.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>

namespace QueryPipeline
{
    Parser::Parser() = default;

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

        LogicalPlan* logicalPlan = nullptr;
        if (response.type() == typeid(SelectStatement)) {
            const auto selectStatement = std::any_cast<SelectStatement>(response);
            Validator::Validate(selectStatement);
            logicalPlan = BuildLogicalPlan(selectStatement);
        }
        else if (response.type() == typeid(CreateDbStatement)) {
            const auto createDbStatement = std::any_cast<CreateDbStatement>(response);
            Validator::Validate(createDbStatement);
            logicalPlan = BuildLogicalPlan(createDbStatement);
        }
        else if (response.type() == typeid(DropDbStatement)) {
            const auto dropDbStatement = std::any_cast<DropDbStatement>(response);
            Validator::Validate(dropDbStatement);

        }

        PhysicalPlan::PhysicalOperator* physicalPlan = PhysicalPlan::BuildPhysicalPlan(logicalPlan);
        const auto results = physicalPlan->Execute();

        delete logicalPlan;
        delete physicalPlan;
    }
}