#include <QueryPipeline/Parser.h>
#include <string>
#include <utility>
#include <CoreEngine/Managers/TransactionManager.h>
#include <Server/Server.h>
#include <QueryPipeline/Cursor.h>
#include <QueryPipeline/LogicalPlan.h>
#include <QueryPipeline/ErrorListener.h>
#include <thread>

#include <iostream>

#include <Systemic/CancellationToken.h>
#include <QueryPipeline/CompileContext.h>
#include <QueryPipeline/Parsing/Lexer.h>
#include <QueryPipeline/Parsing/SqlParser.h>


namespace QueryPipeline{
    QueryContext::QueryContext()
        : _referencedColumns(), status(this->_compileContext.GetAllocator()), _session(nullptr), _nextStatement(0),
          _virtualId(0), hasMore(false)
    {
        this->cursors.SetAllocator(this->_compileContext.GetAllocator());
    }


     QueryContext::QueryContext(Errors::ParserStatus& error)
         : _referencedColumns(), status(std::move(error)), _session(nullptr),
           _nextStatement(0), _virtualId(0), hasMore(false)
     {
         this->cursors.SetAllocator(this->_compileContext.GetAllocator());
     }

    QueryContext::QueryContext(QueryContext&& other) noexcept
        : _referencedColumns(other._referencedColumns),
          _scope(std::move(other._scope)),
          _compileContext(std::move(other._compileContext)),
          _executionContext(std::move(other._executionContext)),
          cursors(std::move(other.cursors)),
          status(std::move(other.status)),
          _session(std::exchange(other._session, nullptr)),
          _nextStatement(std::exchange(other._nextStatement, {})),
          _virtualId(std::exchange(other._virtualId, {})),
          hasMore(std::exchange(other.hasMore, false))
    {}

    QueryContext& QueryContext::operator=(QueryContext&& other) noexcept {
        if (this == &other)
            return *this;

        this->Release();

        this->_referencedColumns = other._referencedColumns;
        this->_scope = std::move(other._scope);
        this->_compileContext = std::move(other._compileContext);
        this->_executionContext = std::move(other._executionContext);
        this->cursors = std::move(other.cursors);
        this->status = std::move(other.status);
        this->_session = std::exchange(other._session, nullptr);
        this->_nextStatement = std::exchange(other._nextStatement, {});
        this->_virtualId = std::exchange(other._virtualId, {});
        this->hasMore = std::exchange(other.hasMore, false);

        return *this;
    }

    static_assert(std::is_nothrow_move_constructible_v<CompileValidationScope>);
    static_assert(std::is_nothrow_move_constructible_v<CompileContext>);
    static_assert(std::is_nothrow_move_constructible_v<CoreEngine::ExecutionContext>);
    static_assert(std::is_nothrow_move_constructible_v<DataStructures::PolymorphicArray<Cursor*>>);

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

    LogicalPlan* Parser::BuildLogicalPlan(QueryContext& result, Statements::Statement* statement){
        auto validation = statement->Compile(result);
        if (!validation.IsOk()) {
            result.status = Errors::ParserStatus(true, validation._message);
            return nullptr;
        }

        auto* logicalPlan = statement->ToLogical(result);

        if (logicalPlan == nullptr) {
            static constexpr DataTypes::StringView ERROR_MESSAGE = "Unexpected error occurred during plan build";
            result.status = Errors::ParserStatus(true, ERROR_MESSAGE, result.GetAllocator());
            return nullptr;
        }

        return logicalPlan;
    }

    PhysicalPlan::PlanNode* Parser::BuildExecutionPlan(QueryContext &result, LogicalPlan *logicalPlan) {
        auto* physicalPlan = logicalPlan->ToPhysical(result);
        if(physicalPlan == nullptr){
            static constexpr DataTypes::StringView ERROR_MESSAGE = "Unexpected error occurred during physical plan build";
            result.status = Errors::ParserStatus(true, ERROR_MESSAGE, result.GetAllocator());
            return nullptr;
        }

        return physicalPlan;
    }

    void Parser::CloseCursor(const session_id_t sessionId, const PipelineConstants::cursor_id_t cursorId) {
        static const auto& server = Network::Server::Get();
        const auto _ = server.CloseCursor(sessionId, cursorId);
    }

    QueryContext Parser::Parse(const session_id_t sessionId, const std::string& query) {
        static auto& server = Network::Server::Get();

        QueryContext context;
        context._session = server.GetSession(sessionId);
        context.CreateValidationScope(context._session);

        const auto* allocator = context.GetAllocator();

        Parsing::Diagnostic diagnostic;
        DataStructures::PolymorphicArray<Parsing::Token> tokens(allocator);
        const auto tokenizeStatus = Parsing::Tokenize(DataTypes::StringView(query), tokens, diagnostic);
        if (!tokenizeStatus){
            context.status = Errors::ParserStatus(true, diagnostic.message, allocator);
            return context;
        }

        const auto* session = server.GetSession(sessionId);

        if (session == nullptr){
            static constexpr DataTypes::StringView ERROR_MESSAGE = "Session not found";
            context.status = Errors::ParserStatus(true, ERROR_MESSAGE, allocator);
            return context;
        }

        Parsing::SqlParser parser(tokens, allocator, diagnostic);
        auto* statements = context._compileContext.GetStatements();
        if (!parser.ParseStatements(statements, session->databaseId)){
            context.status = Errors::ParserStatus(true, diagnostic.message, allocator);
        }

        context.cursors.Reserve(statements->Size());
        return context;
    }

    Cursor* Parser::CompileNext(QueryContext& context, session_id_t sessionId){
        static const auto& server = Network::Server::Get();
        static auto& transactionManager = CoreEngine::TransactionManager::Get();

        const auto* statements = context._compileContext.GetStatements();
        if (context._nextStatement >= statements->Size())
            return nullptr;

        auto* statement = (*statements)[context._nextStatement++];
        statement->databaseId = server.GetSession(sessionId)->databaseId;

        auto* logicalPlan = Parser::BuildLogicalPlan(context, statement);
        if (context.status.hasError)
            return nullptr;

        auto* physicalPlan = Parser::BuildExecutionPlan(context, logicalPlan);

        if (context.status.hasError)
            return nullptr;

        auto snapshot = transactionManager.BeginTransaction(sessionId);

        std::cout   << "Executing transaction: " << snapshot.transactionId
            << " by thread: " << std::this_thread::get_id()
            << std::endl;

        //TODO set batch size correctly
        CoreEngine::ExecutionContext executionContext(
            snapshot, Constants::DEFAULT_BATCH_SIZE,
            &context._session->variables
        );

        return server.CreateCursor(
            sessionId, context._compileContext,
            executionContext, physicalPlan
        );
    }

    void Parser::CommitTransaction(const session_id_t sessionId, const Cursor* cursor) {
        static auto& transactionManager = CoreEngine::TransactionManager::Get();
        transactionManager.CommitTransaction(cursor->GetSnapshot());
        Parser::CloseCursor(sessionId, cursor->GetId());
    }

    void Parser::RollbackTransaction(const session_id_t sessionId, const Cursor* cursor) {
        static auto& transactionManager = CoreEngine::TransactionManager::Get();
        transactionManager.RollbackTransaction(cursor->GetExecutionContext());
        Parser::CloseCursor(sessionId, cursor->GetId());
    }
}
