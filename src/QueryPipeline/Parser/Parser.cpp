#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../../Database/Column/Column.h"
#include "../../Server/Server.h"
#include "../Cursor/Cursor.h"
#include "../ErrorListener/ErrorListener.h"
#include "../Visitor/Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../PhysicalPlan/PhysicalPlan.h"

#include <SQLBaseListener.h>
#include <SQLLexer.h>

namespace QueryPipeline
{
    Parser::Parser() = default;

    Statements::Statement* Parser::CreateStatement(const std::any &ast, const DataTypes::Guid& sessionId){
        function<Statements::Statement *(const any &)> handler;

        if (!handlers.TryGetValue(ast.type(), handler))
            return nullptr;

        Statements::Statement* statement = handler(ast);

        const auto* session = Server::ServerInstance::Get().GetSession(sessionId);

        if (session == nullptr) {
            delete statement;
            return nullptr;
        }

        statement->databaseId = session->databaseId;
        statement->sessionId = session->sessionId;
        return statement;
    }

    void Parser::ClearQuery(const Statements::Statement *statement, const LogicalPlan *logicalPlan) {
        delete statement;
        delete logicalPlan;
    }

    Parser::~Parser() = default;

    void Parser::Parse(const string& query, const DataTypes::Guid& sessionId){
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

            statement = CreateStatement(response, sessionId);
        }
        catch (const exception& e) {
            std::cerr << "Parser exception: " << e.what() << std::endl;
            Parser::ClearQuery(statement, nullptr);
            return;
        }

        if (statement == nullptr || !statement->ValidateStatement()) {
            Parser::ClearQuery(statement, nullptr);
            return;
        }

        auto* logicalPlan = statement->ToLogical();
        
        if (logicalPlan == nullptr) {
            Parser::ClearQuery(statement, logicalPlan);
            return;
        }

        auto* physicalPlan = logicalPlan->ToPhysical();

        if(physicalPlan == nullptr){
            Parser::ClearQuery(statement, logicalPlan);
            return;
        }

        PhysicalPlan::PhysicalPlanResult* result = nullptr;

        const auto& server = Server::ServerInstance::Get();

        auto* cursor = server.CreateCursor(sessionId, physicalPlan);

        while (cursor->hasMore()) {
            result = cursor->fetchNextBatch();

            if (result == nullptr) {
                Parser::ClearQuery(statement, logicalPlan);
                return;
            }

            if (result->code != Errors::ResultCode::Ok) {
                std::cerr << result->message << std::endl;

                Parser::ClearQuery(statement, logicalPlan);
                return;
            }

            for (const auto& column : result->displayColumnNames)
                std::cout << column << " || ";

            std::cout << std::endl;

            for (const auto& row: result->results)
                row.Print();
        }

        const auto _ = server.CloseCursor(sessionId);
        Parser::ClearQuery(statement, logicalPlan);
    }
}