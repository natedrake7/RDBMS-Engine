#pragma once
#include "../Cursor/Cursor.h"
#include "../PhysicalPlan/PhysicalPlan.h"
#include "../Statements/Statements.h"
#include <any>
#include <functional>
#include <typeindex>


namespace QueryPipeline {
class Cursor;}using namespace std;

namespace QueryPipeline{

        static Dictionary<std::type_index, function<Statements::Statement*(const std::any&)>> handlers = {
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
        };

    struct ParserResult {
        Errors::Error status;
        std::vector<QueryResult> rows;
        std::vector<std::string> columns;
        Cursor* cursor;

        bool hasMore;

        ParserResult() {
            this->hasMore = false;
            this->cursor = nullptr;
        }

        explicit ParserResult(const Errors::Error& error);
    };

    class Parser{
        static Statements::Statement* CreateStatement(const std::any &ast, const DataTypes::Guid& sessionId);
        static void ClearQuery(const Statements::Statement* statement,const LogicalPlan* logicalPlan);

        static Statements::Statement* Parse(ParserResult& result, const DataTypes::Guid& sessionId, const std::string& query);
        static PhysicalPlan::PhysicalOperator* BuildExecutionPlan(ParserResult& result, Statements::Statement* statement);
        static void CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId);

        public:
            Parser();
            ~Parser();

            static Parser& Get()
            {
                static Parser instance;
                return instance;
            }

            static ParserResult StartTransaction(const string& query, const DataTypes::Guid& sessionId);

            static ParserResult Execute(Cursor* cursor);

            static void CommitTransaction(const DataTypes::Guid& sessionId, const PhysicalPlan::Snapshot& snapshot);
            static void RollbackTransaction(const DataTypes::Guid& sessionId, const PhysicalPlan::Snapshot& snapshot);
    };

}