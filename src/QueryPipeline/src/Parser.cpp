#include "../include/Parser.h"
#include <string>
#include <utility>
#include "../../CoreEngine/include/Managers/TransactionManager.h"
#include "../../Server/include/Server.h"
#include "../include/Cursor.h"
#include "../include/LogicalPlan.h"
#include "../include/ErrorListener.h"
#include <thread>

#include <iostream>

#include "../include/CompileContext.h"
#include "Parsing/Lexer.h"
#include "Parsing/SqlParser.h"


namespace QueryPipeline{
    QueryContext::QueryContext()
        : status(this->_compileContext.GetAllocator()), _session(nullptr), _virtualId(0),
          hasMore(false)
    {
        this->cursors.SetAllocator(this->_compileContext.GetAllocator());
    }


     void QueryContext::CreateValidationScope(const Network::Session* session){
        for (const auto& [key, variable] : session->variables){
            this->_scope.variables.ForceAdd(key, variable->GetType());
        }
    }

     const ::Memory::IAllocator* QueryContext::GetAllocator() const{
        return this->_compileContext.GetAllocator();
     }

     UnsignedSmallInt QueryContext::NextVirtualId(){
        return this->_virtualId++;
     }

     void QueryContext::Release() const{
        this->_compileContext.GetAllocator()->Release();
     }

     QueryContext::QueryContext(Errors::Error& error)
         : status(std::move(error)), _session(nullptr),
           _virtualId(0), hasMore(false)
     {
         this->cursors.SetAllocator(this->_compileContext.GetAllocator());
     }

    QueryContext::QueryContext(QueryContext&& other) noexcept
        : _scope(std::move(other._scope)), _compileContext(std::move(other._compileContext)),
          cursors(std::move(other.cursors)), status(std::move(other.status)), _session(nullptr),
          _virtualId(other._virtualId), hasMore(other.hasMore)
    {}

    QueryContext& QueryContext::operator=(QueryContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->status = other.status;
        this->hasMore = other.hasMore;
        this->_scope = std::move(other._scope);
        this->cursors = std::move(other.cursors);
        this->_session = other._session;
        this->_virtualId = other._virtualId;

        return *this;
    }

    void Parser::Parse(QueryContext& result, const DataTypes::Guid& sessionId, const std::string& query) {
        Parsing::Diagnostic diagnostic;
        DataStructures::PolymorphicArray<Parsing::Token> tokens(result.GetAllocator());
        const auto tokenizeStatus = Parsing::Tokenize(DataTypes::StringView(query), tokens, diagnostic);

        if (!tokenizeStatus){
            result.status = Errors::Error(true, diagnostic.message, result.GetAllocator());
            return;
        }

        static auto& server = Network::Server::Get();
        const auto* session = server.GetSession(sessionId);

        Parsing::SqlParser parser(tokens, result.GetAllocator(), diagnostic);
        if (!parser.ParseStatements(result._compileContext.GetStatements(), &sessionId, session->databaseId)){
            result.status = Errors::Error(true, diagnostic.message, result.GetAllocator());
        }
     }

    LogicalPlan* Parser::BuildLogicalPlan(QueryContext& result, Statements::Statement* statement){
        auto validation = statement->Compile(result);
        if (!validation.IsOk()) {
            result.status = Errors::Error(true, validation.message);
            return nullptr;
        }

        auto* logicalPlan = statement->ToLogical(result);

        if (logicalPlan == nullptr) {
            static constexpr DataTypes::StringView errorMsg = "Unexpected error occurred during plan build";
            result.status = Errors::Error(true, errorMsg, result.GetAllocator());
            return nullptr;
        }

        return logicalPlan;
    }

    PhysicalPlan::PlanNode* Parser::BuildExecutionPlan(QueryContext &result, LogicalPlan *logicalPlan) {
        auto* physicalPlan = logicalPlan->ToPhysical(result);
        if(physicalPlan == nullptr){
            static constexpr DataTypes::StringView errorMsg = "Unexpected error occurred during physical plan build";
            result.status = Errors::Error(true, errorMsg, result.GetAllocator());
            return nullptr;
        }

        return physicalPlan;
    }

    void Parser::CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, const PipelineConstants::cursor_id_t cursorId) {
        static const auto& server = Network::Server::Get();
        const auto _ = server.CloseCursor(sessionId, cursorId);
    }

    QueryContext Parser::StartTransaction(const std::string &query, const DataTypes::Guid &sessionId){
        static const auto& server = Network::Server::Get();
        static auto& transactionManager = CoreEngine::TransactionManager::Get();

        QueryContext queryContext;
        queryContext._session = server.GetSession(sessionId);
        queryContext.CreateValidationScope(queryContext._session);
        Parser::Parse(queryContext, sessionId, query);

        if (queryContext.status.hasError)
            return queryContext;

        queryContext.status.hasError = false;

        const auto* statements = queryContext._compileContext.GetStatements();
        queryContext.cursors.Reserve(statements->Size());
        for (auto* statement: *statements) {
            auto* logicalPlan = Parser::BuildLogicalPlan(queryContext, statement);
            if (queryContext.status.hasError)
                break;

            auto* physicalPlan = Parser::BuildExecutionPlan(queryContext, logicalPlan);

            if (queryContext.status.hasError)
                break;

            auto snapshot = transactionManager.BeginTransaction(sessionId);

            std::cout   << "Executing transaction: " << snapshot.transactionId
                        << " by thread: " << std::this_thread::get_id()
                        << std::endl;

            //TODO set batch size correctly
            CoreEngine::ExecutionContext executionContext(
                snapshot, Constants::DEFAULT_BATCH_SIZE,
                &queryContext._session->variables
            );

            queryContext.cursors.Push(server.CreateCursor(
                sessionId, queryContext._compileContext,
                executionContext, physicalPlan
            ));
        }

        return queryContext;
    }

    void Parser::CommitTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor) {
        static auto& transactionManager = CoreEngine::TransactionManager::Get();
        transactionManager.CommitTransaction(cursor->GetSnapshot());
        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }

    void Parser::RollbackTransaction(const DataTypes::Guid &sessionId, const Cursor* cursor) {
        static auto& transactionManager = CoreEngine::TransactionManager::Get();
        transactionManager.RollbackTransaction(cursor->GetExecutionContext());
        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }
}
