#include "../include/Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include <typeindex>
#include "../../CoreEngine/include/Managers/TransactionManager.h"
#include "../../Server/include/Server.h"
#include "../include/Cursor.h"
#include "../include/Visitor.h"
#include "../include/LogicalPlan.h"
#include "../include/ErrorListener.h"
#include <thread>

#include <SQLBaseListener.h>
#include <SQLLexer.h>

#include "../include/CompileContext.h"


namespace QueryPipeline{
    static Dictionary<std::type_index, std::function<Statements::Statement*(const std::any&)>> handlers = {
    {
        typeid(Statements::CreateUserStatement*),
        [](const auto& r) { return std::any_cast<Statements::CreateUserStatement*>(r); }
        },
    {
        typeid(Statements::GrantRoleStatement*),
        [](const auto& r) { return std::any_cast<Statements::GrantRoleStatement*>(r); }
        },

        {
            typeid(Statements::CreateDbStatement*),
            [](const auto& r) { return std::any_cast<Statements::CreateDbStatement*>(r); }
        },

        {
            typeid(Statements::UseDatabaseStatement*),
            [](const auto& r) { return std::any_cast<Statements::UseDatabaseStatement*>(r); }
        },

        {
            typeid(Statements::DropDbStatement*),
            [](const auto& r) { return std::any_cast<Statements::DropDbStatement*>(r); }
        },

        {
            typeid(Statements::CreateSchemaStatement*),
            [](const auto& r) { return std::any_cast<Statements::CreateSchemaStatement*>(r); }
        },

        {
            typeid(Statements::SelectStatement*),
            [](const auto& r) { return std::any_cast<Statements::SelectStatement*>(r); }
        },
        {
            typeid(Statements::CreateTableStatement*),
            [](const auto& r) { return std::any_cast<Statements::CreateTableStatement*>(r); }
        },
        {
            typeid(Statements::InsertStatement*),
            [](const auto& r) { return std::any_cast<Statements::InsertStatement*>(r); }
        },

        {
            typeid(Statements::DeleteStatement*),
            [](const auto& r) { return std::any_cast<Statements::DeleteStatement*>(r); }
        },

        {
            typeid(Statements::UpdateStatement*),
            [](const auto& r) { return std::any_cast<Statements::UpdateStatement*>(r); }
        },

        {
            typeid(Statements::CreateIndexStatement*),
            [](const auto& r) { return std::any_cast<Statements::CreateIndexStatement*>(r); }
        },

        {
            typeid(Statements::AlterTableStatement*),
            [](const auto& r) { return std::any_cast<Statements::AlterTableStatement*>(r); }
        },

        {
            typeid(Statements::DeclareVariableStatement*),
            [](const auto& r) { return std::any_cast<Statements::DeclareVariableStatement*>(r); }
        },

        {
            typeid(Statements::SetVariableStatement*),
            [](const auto& r) { return std::any_cast<Statements::SetVariableStatement*>(r); }
        },
    };

    Parser::Parser() = default;

    QueryContext::QueryContext()
        :   status(this->_compileContext.GetAllocator()), hasMore(false),
            _executionMode(Constants::ExecutionMode::Row){
        this->cursors.SetAllocator(this->_compileContext.GetAllocator());
    }

     void QueryContext::CreateValidationScope(const Dictionary<DataTypes::String, Variable>& sessionVariables){
        for (const auto& [key, variable] : sessionVariables)
            this->_scope.variables.ForceAdd(key, variable.GetType());
    }

     const ::Memory::IAllocator* QueryContext::GetAllocator() const{
        return this->_compileContext.GetAllocator();
     }

     QueryContext::QueryContext(const Errors::Error &error){
        this->status = error;
        this->hasMore = false;
        this->_executionMode = Constants::ExecutionMode::Row;
        this->cursors.SetAllocator(this->_compileContext.GetAllocator());
    }

    QueryContext::QueryContext(QueryContext&& other) noexcept{
        this->status = other.status;
        this->hasMore = other.hasMore;
        this->_scope = std::move(other._scope);
        this->_executionMode = other._executionMode;
        this->cursors = std::move(other.cursors);
    }

    QueryContext& QueryContext::operator=(QueryContext&& other) noexcept{
        if (this == &other) return *this;

        this->status = other.status;
        this->hasMore = other.hasMore;
        this->_scope = std::move(other._scope);
        this->_executionMode = other._executionMode;
        this->cursors = std::move(other.cursors);

        return *this;
    }

    void Parser::CreateStatements(
        CompileContext& context,
        const std::any &queries,
        const DataTypes::Guid& sessionId
    ){
        const auto castQueries = std::any_cast<DataStructures::PolymorphicArray<std::any>>(queries);
        const auto* session = Network::Server::Get().GetSession(sessionId);

         context.Reserve(castQueries.Size());
         for (const auto& query: castQueries) {
             std::function<Statements::Statement *(const std::any &)> handler;

             if (!handlers.TryGetValue(query.type(), handler))
                 return;

             Statements::Statement* statement = handler(query);

            if (session == nullptr)
                continue;

            statement->databaseId = session->databaseId;
            statement->sessionId = session->sessionId;

            context.Push(statement);
         }
    }

    Parser::~Parser() = default;

    void Parser::Parse(QueryContext& result, const DataTypes::Guid& sessionId, const std::string& query) {
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
        try {
            auto* tree = parser.sqlStatement();
            SQLVisitorImplementation visitor(result._compileContext);

            const auto response = visitor.visit(tree);
            CreateStatements(result._compileContext, response, sessionId);
        }
        catch (const std::exception& e) {
            // Parser::ClearQuery(statements);
            std::ostringstream os;
            os << "Parser exception: " << e.what();

            auto str = DataTypes::String::Concat(result.GetAllocator(), "Parser exception: ", e.what());
            result.status = Errors::Error(true, std::move(str));
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
        const auto* session = server.GetSession(sessionId);

        queryContext.CreateValidationScope(session->variables);
        Parser::Parse(queryContext, sessionId, query);

        queryContext.status.hasError = false;
        if (queryContext.status.hasError)
            return queryContext;

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
                snapshot, 10000,
                session->variables, queryContext._executionMode
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
