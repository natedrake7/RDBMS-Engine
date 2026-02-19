#include "../include/Parser.h"
#include <string>
#include <vector>
#include "SQLParser.h"
#include <typeindex>
#include "../../DatabaseEngine/include/Managers/TransactionManager.h"
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

    CompileResult::CompileResult(){
        this->hasMore = false;
        this->cursors.SetAllocator(this->_context.GetAllocator());
    }

     void CompileResult::CreateValidationScope(const Dictionary<std::string, Variable>& sessionVariables){
        for (const auto& [key, variable] : sessionVariables)
            this->_scope.variables.ForceAdd(key, variable.GetType());
    }

    CompileResult::CompileResult(const Errors::Error &error){
        this->status = error;
        this->hasMore = false;
        this->cursors.SetAllocator(this->_context.GetAllocator());
        // this->columns.SetAllocator(this->_context.GetAllocator());
        // this->rows.SetAllocator(this->_context.GetAllocator());
    }

    CompileResult::CompileResult(CompileResult&& other) noexcept{
         this->status = other.status;
         this->hasMore = other.hasMore;
         this->_scope = std::move(other._scope);
         // this->columns = std::move(other.columns);
         // this->rows = std::move(other.rows);
         this->cursors = std::move(other.cursors);
    }

    CompileResult& CompileResult::operator=(CompileResult&& other) noexcept{
         if (this == &other) return *this;

         this->status = other.status;
         this->hasMore = other.hasMore;
         this->_scope = std::move(other._scope);
         // this->columns = std::move(other.columns);
         // this->rows = std::move(other.rows);
         this->cursors = std::move(other.cursors);

         return *this;
    }

    void Parser::CreateStatements(
        CompileContext& context,
        const std::any &queries,
        const DataTypes::Guid& sessionId
    ){
        const auto castQueries = std::any_cast<std::vector<std::any>>(queries);
        const auto* session = Network::Server::Get().GetSession(sessionId);

         context.Reserve(castQueries.size());
         for (const auto& query: castQueries) {
             std::function<Statements::Statement *(const std::any &)> handler;

             if (!handlers.TryGetValue(query.type(), handler))
                 return;

             Statements::Statement* statement = handler(query);

            if (session == nullptr) {
                // statement->CleanUp();
                continue;
            }

            statement->databaseId = session->databaseId;
            statement->sessionId = session->sessionId;

            context.Push(statement);
         }
    }

    Parser::~Parser() = default;

    void Parser::Parse(CompileResult& result, const DataTypes::Guid& sessionId, const std::string& query) {
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
            SQLVisitorImplementation visitor(result._context);

            const auto response = visitor.visit(tree);
            CreateStatements(result._context, response, sessionId);
        }
        catch (const std::exception& e) {
            // Parser::ClearQuery(statements);

            std::ostringstream os;
            os << "Parser exception: " << e.what();

            result.status = Errors::Error(true, os.str());
        }
     }

    LogicalPlan* Parser::BuildLogicalPlan(CompileResult& result, Statements::Statement* statement){
        auto validation = statement->Compile(result);
        if (!validation.IsOk()) {
            result.status = Errors::Error(true, validation.message);
            return nullptr;
        }

        auto* logicalPlan = statement->ToLogical(result);

        if (logicalPlan == nullptr) {
            result.status = Errors::Error(true, "Unexpected error occurred during plan build");
            return nullptr;
        }

        return logicalPlan;
    }

    PhysicalPlan::ExecutionNode* Parser::BuildExecutionPlan(CompileResult &result, LogicalPlan *logicalPlan) {
        auto* physicalPlan = logicalPlan->ToPhysical(result);
        if(physicalPlan == nullptr){
            result.status = Errors::Error(true, "Unexpected error occurred during physical plan build");
            return nullptr;
        }

        return physicalPlan;
    }

    void Parser::CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, const PipelineConstants::cursor_id_t cursorId) {
        static const auto& server = Network::Server::Get();
        const auto _ = server.CloseCursor(sessionId, cursorId);
    }

    CompileResult Parser::StartTransaction(const std::string &query, const DataTypes::Guid &sessionId){
        static const auto& server = Network::Server::Get();
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        CompileResult result;
        const auto* session = server.GetSession(sessionId);

        result.CreateValidationScope(session->variables);
        Parser::Parse(result, sessionId, query);

        result.status.hasError = false;
        if (result.status.hasError)
            return result;

        const auto* statements = result._context.GetStatements();
        result.cursors.Reserve(statements->Size());
        for (auto* statement: *statements) {
            auto* logicalPlan = Parser::BuildLogicalPlan(result, statement);
            if (result.status.hasError)
                break;

            auto* physicalPlan = Parser::BuildExecutionPlan(result, logicalPlan);

            if (result.status.hasError)
                break;

            const auto snapshot = transactionManager.BeginTransaction(sessionId);

            std::cout   << "Executing transaction: " << snapshot.transactionId
                        << " by thread: " << std::this_thread::get_id()
                        << std::endl;

            DatabaseEngine::ExecutionProperties properties(snapshot, 10000, session->variables);

            //for test
            // if (dynamic_cast<Statements::SelectStatement *>(statement) != nullptr) {
            //     std::this_thread::sleep_for(5000ms);
            // }

            result.cursors.Push(server.CreateCursor(sessionId, properties, physicalPlan));
        }

        // if (result.status.hasError)
        //     Parser::ClearQuery(statements);

        return result;
    }

    void Parser::CommitTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.CommitTransaction(cursor->GetSnapshot());

        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }

    void Parser::RollbackTransaction(const DataTypes::Guid &sessionId, const Cursor* cursor) {
        static auto& transactionManager = DatabaseEngine::TransactionManager::Get();

        transactionManager.RollbackTransaction(cursor->GetSnapshot());

        Parser::CleanUpPostExecutionObjects(sessionId, cursor->GetId());
    }
}
