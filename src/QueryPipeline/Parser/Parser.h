#pragma once
#include "../Statements/Statements.h"
#include <any>
#include <functional>
#include <typeindex>


using namespace std;

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

    class Parser{
        ~Parser();
        Parser();

        static Statements::Statement* CreateStatement(const std::any &ast, const DataTypes::Guid& sessionId);

        public:
            static Parser& Get()
            {
                static Parser instance;

                return instance;
            }

            static void Parse(const string& query, const DataTypes::Guid& sessionId);
    };

}