#pragma once
#include "../Statements/Statements.h"
#include <any>
#include <functional>
#include <typeindex>


using namespace std;

namespace QueryPipeline{

        static Dictionary<std::type_index, function<Statements::Statement*(const std::any&)>> handlers = {
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
                typeid(Statements::CreateDbStatement*),
                [](const auto& r) { return std::any_cast<Statements::CreateDbStatement*>(r); }
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
                typeid(Statements::DeleteStatement*),
                [](const auto& r) { return std::any_cast<Statements::DeleteStatement*>(r); }
            },

            {
                typeid(Statements::UpdateStatement*),
                [](const auto& r) { return std::any_cast<Statements::UpdateStatement*>(r); }
            },
        };

    class Parser{
        ~Parser();
        Parser();

        static Statements::Statement* CreateStatement(const std::any &ast, const std::string& dbName);

        public:
            static Parser& Get()
            {
                static Parser instance;

                return instance;
            }

            static void Parse(const string& query, const std::string& dbName);
    };

}