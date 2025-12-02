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
     }

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

    Statements::Statement* Parser::Parse(ParserResult& result, const DataTypes::Guid& sessionId, const std::string& query) {
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
        Statements::Statement* statement = nullptr;

        try {
            SQLParser::SqlStatementContext *tree = parser.sqlStatement();

            SQLVisitorImplementation visitor;

            const auto response = visitor.visit(tree);

            statement = CreateStatement(response, sessionId);
        }
        catch (const exception& e) {
            Parser::ClearQuery(statement, nullptr);

            ostringstream os;
            os << "Parser exception: " << e.what();

            result.status = {true, os.str()};
            return nullptr;
        }

        return statement;
     }

    PhysicalPlan::PhysicalOperator * Parser::BuildExecutionPlan(ParserResult &result, Statements::Statement *statement) {
        auto validation = statement->ValidateStatement();
        if (!validation.IsOk()) {
            Parser::ClearQuery(statement, nullptr);

            result.status  = {true, validation.message};
            return nullptr;
        }

        auto* logicalPlan = statement->ToLogical();

        if (logicalPlan == nullptr) {
            Parser::ClearQuery(statement, logicalPlan);

            result.status  = {true, "Unexpected error occurred during plan build"};
             return nullptr;
        }

        auto* physicalPlan = logicalPlan->ToPhysical();
        Parser::ClearQuery(statement, logicalPlan);

        if(physicalPlan == nullptr){
            result.status  = {true, "Unexpected error occurred during physical plan build"};
            return nullptr;
        }

        return physicalPlan;
    }

    void Parser::CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId) {
        static const auto& server = Server::ServerInstance::Get();

        const auto _ = server.CloseCursor(sessionId);
    }

    ParserResult Parser::StartTransaction(const string &query, const DataTypes::Guid &sessionId){
        ParserResult result;

        auto* statement = Parser::Parse(result, sessionId, query);

        if (result.status.hasError)
            return result;

        auto* physicalPlan = Parser::BuildExecutionPlan(result, statement);

        if (result.status.hasError)
            return result;

        static const auto& server = Server::ServerInstance::Get();
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        const auto snapshot = transactionManager.BeginTransaction(sessionId);

        std::cout   << "Executing transaction: " << snapshot.transactionId
                    << " by thread: " << std::this_thread::get_id()
                    << std::endl;

        const PhysicalPlan::PhysicalPlanExecutionProperties properties{
            snapshot,
            1000
        };

        //for test
        // if (dynamic_cast<Statements::SelectStatement *>(statement) != nullptr) {
        //     std::this_thread::sleep_for(5000ms);
        // }

        result.cursor = server.CreateCursor(sessionId, properties, physicalPlan);

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

    void Parser::CommitTransaction(const DataTypes::Guid& sessionId, const PhysicalPlan::Snapshot& snapshot) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.CommitTransaction(snapshot);

        Parser::CleanUpPostExecutionObjects(sessionId);
    }

    void Parser::RollbackTransaction(const DataTypes::Guid &sessionId, const PhysicalPlan::Snapshot &snapshot) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.RollbackTransaction(snapshot);

        Parser::CleanUpPostExecutionObjects(sessionId);
    }
}