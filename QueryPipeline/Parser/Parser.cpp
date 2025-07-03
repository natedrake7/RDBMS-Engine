#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../Cursor/Cursor.h"
#include "../ErrorListener/ErrorListener.h"
#include "../Visitor/Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../PhysicalPlan/PhysicalPlan.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>
#include <variant>

namespace QueryPipeline
{
    Parser::Parser() = default;

    Statements::Statement* Parser::CreateStatement(const std::any &ast, const int32_t & databaseId){
        function<Statements::Statement *(const any &)> handler;

        if (!handlers.TryGetValue(ast.type(), handler))
            return nullptr;

        Statements::Statement* statement = handler(ast);
        statement->databaseId = databaseId;
        
        return statement;
    }

    Parser::~Parser() = default;

    void Parser::Parse(const string& query, const int32_t & databaseId){
        // Create an ANTLR input stream from the file
        antlr4::ANTLRInputStream input(query);

        // Create a lexer for the input stream
        SQLLexer lexer(&input);

        // Create a token stream for the lexer
        antlr4::CommonTokenStream tokens(&lexer);

        // Create the parser, passing the token stream
        SQLParser parser(&tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener()); // Add custom

        // Start parsing, typically using the start rule of the grammar
        Statements::Statement* statement = nullptr;

        try {
            SQLParser::SqlStatementContext *tree = parser.sqlStatement();

            SQLVisitorImplementation visitor;

            const auto response = visitor.visit(tree);

            statement = CreateStatement(response, databaseId);
        }
        catch (const exception& e) {
            std::cerr << "Parser exception: " << e.what() << std::endl;
            delete statement;
            return;
        }

        if (!statement->Validate()) {
            delete statement;
            return;
        }

        LogicalPlan* logicalPlan = statement->ToLogical();
        
        if (logicalPlan == nullptr)
            return;

        PhysicalPlan::PhysicalOperator* physicalPlan = logicalPlan->ToPhysical();

        if(physicalPlan == nullptr){
          delete statement;
          delete logicalPlan;
          delete physicalPlan;
          return;
        }

        Cursor cursor(0, physicalPlan, 100);

        PhysicalPlan::PhysicalPlanResult* result = nullptr;

        while (cursor.hasMore()) {
            result = cursor.fetchNextBatch();

            if (result == nullptr) {
                delete result;
                delete statement;
                delete logicalPlan;
                return;
            }

            if (result->code != AdditionalDataTypes::ResultCode::Ok) {
                cerr << result->message << endl;

                delete result;
                delete statement;
                delete logicalPlan;
                return;
            }

            for (const auto& row: result->rows)
                row.PrintRow();

            std::cout << "Printing next batch" << std::endl;
        }

        delete result;
        delete statement;
        delete logicalPlan;
    }
}