#include "Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include "../../Database/TransactionManager/TransactionManager.h"
#include "../../Server/Server.h"
#include "../Cursor/Cursor.h"
#include "../ErrorListener/ErrorListener.h"
#include "../Visitor/Visitor.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../PhysicalPlan/PhysicalPlan.h"
#include <thread>

#include <SQLBaseListener.h>
#include <SQLLexer.h>

namespace QueryPipeline
{
    Parser::Parser() = default;

     ParserResult::ParserResult(const Errors::Error &error){
        this->status = error;
        this->hasMore = false;
     }

    std::vector<Statements::Statement*> Parser::CreateStatement(const std::any &queries, const DataTypes::Guid& sessionId){
        function<Statements::Statement *(const any &)> handler;

        std::vector<Statements::Statement*> statements;

        const auto castQueries = std::any_cast<std::vector<std::any>>(queries);
        const auto* session = Server::ServerInstance::Get().GetSession(sessionId);

         for (const auto& query: castQueries) {

             if (!handlers.TryGetValue(query.type(), handler))
                 return {};

             Statements::Statement* statement = handler(query);

            if (session == nullptr) {
                delete statement;
                continue;
            }

            statement->databaseId = session->databaseId;
            statement->sessionId = session->sessionId;

            statements.push_back(statement);
         }

        return statements;
    }

    void Parser::ClearQuery(const std::vector<Statements::Statement*>& statements, const LogicalPlan *logicalPlan) {
         for (const auto* statement: statements)
             delete statement;

        delete logicalPlan;
    }

    Parser::~Parser() = default;

    std::vector<Statements::Statement*> Parser::Parse(ParserResult& result, const DataTypes::Guid& sessionId, const std::string& query) {
        static auto errorListener = ErrorListener();
        // Create an ANTLR input stream from the file
        antlr4::ANTLRInputStream input(query);

        // Create a lexer for the input stream
        SQLLexer lexer(&input);

        // Create a token stream for the lexer
        antlr4::CommonTokenStream tokens(&lexer);

        // Create the parser, passing the token stream
        SQLParser parser(&tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(&errorListener); // Add custom

        // Start parsing, typically using the start rule of the grammar
        std::vector<Statements::Statement*> statements;
        try {
            SQLParser::SqlStatementContext *tree = parser.sqlStatement();

            SQLVisitorImplementation visitor;

            const auto response = visitor.visit(tree);

            statements = CreateStatement(response, sessionId);
        }
        catch (const exception& e) {
            Parser::ClearQuery(statements, nullptr);

            ostringstream os;
            os << "Parser exception: " << e.what();

            result.status = {true, os.str()};
            return {};
        }

        return statements;
     }

    PhysicalPlan::PhysicalOperator * Parser::BuildExecutionPlan(ParserResult &result, Statements::Statement *statement) {
        auto validation = statement->ValidateStatement();
        if (!validation.IsOk()) {
            result.status  = {true, validation.message};
            return nullptr;
        }

        auto* logicalPlan = statement->ToLogical();

        if (logicalPlan == nullptr) {
            result.status  = {true, "Unexpected error occurred during plan build"};
            return nullptr;
        }

        auto* physicalPlan = logicalPlan->ToPhysical();
        if(physicalPlan == nullptr){
            result.status  = {true, "Unexpected error occurred during physical plan build"};
            return nullptr;
        }

        return physicalPlan;
    }

    void Parser::CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, const QueryPipeline::PipelineConstants::cursor_id_t& cursorId) {
        static const auto& server = Server::ServerInstance::Get();

        const auto _ = server.CloseCursor(sessionId, cursorId);
    }

    ParserResult Parser::StartTransaction(const string &query, const DataTypes::Guid &sessionId){
        ParserResult result;

        const auto statements = Parser::Parse(result, sessionId, query);

        if (result.status.hasError)
            return result;

        static const auto& server = Server::ServerInstance::Get();
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        for (auto* statement: statements) {
            auto* physicalPlan = Parser::BuildExecutionPlan(result, statement);

            if (result.status.hasError) {
                break;
            }

            const auto snapshot = transactionManager.BeginTransaction(sessionId);

            std::cout   << "Executing transaction: " << snapshot.transactionId
                        << " by thread: " << std::this_thread::get_id()
                        << std::endl;

            const auto* session = server.GetSession(sessionId);

            const PhysicalPlan::PhysicalPlanExecutionProperties properties(snapshot,1000, session->variables);

            //for test
            // if (dynamic_cast<Statements::SelectStatement *>(statement) != nullptr) {
            //     std::this_thread::sleep_for(5000ms);
            // }

            result.cursors.push_back(server.CreateCursor(sessionId, properties, physicalPlan));
        }

        if (result.status.hasError)
            Parser::ClearQuery(statements, nullptr);

        return result;
    }

    ParserResult Parser::Execute(Cursor* cursor){
        ParserResult result;
        //return the cursor to allow the thread to fetch more
        auto* executionResult = cursor->fetchNextBatch();

        if (executionResult == nullptr) {
            result.status  = {false, "Command completed Successfully"};
            return result;
        }

        if (!executionResult->IsOk()){
            result.status  = {true, executionResult->message};
            return result;
        }

        result.columns = std::move(executionResult->displayColumnNames);
        result.rows = std::move(executionResult->results);

        return result;
    }

    void Parser::CommitTransaction(const DataTypes::Guid& sessionId, const QueryPipeline::Cursor* cursor) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.CommitTransaction(cursor->GetSnapshot());

        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }

    void Parser::RollbackTransaction(const DataTypes::Guid &sessionId, const QueryPipeline::Cursor* cursor) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.RollbackTransaction(cursor->GetSnapshot());

        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }
}