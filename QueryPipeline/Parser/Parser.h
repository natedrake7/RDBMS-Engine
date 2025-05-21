#pragma once
#include "../Statements/Statements.h"
#include <any>
#include <functional>
#include <typeindex>


using namespace std;

namespace QueryPipeline{

        static Dictionary<std::type_index, function<Statements::Statement*(const std::any&)>> handlers = {
            {
                typeid(Statements::SelectStatement),
                [](const auto& r) { return new Statements::SelectStatement(std::any_cast<Statements::SelectStatement>(r)); }
            },
            {
                typeid(Statements::CreateTableStatement),
                [](const auto& r) { return new Statements::CreateTableStatement(std::any_cast<Statements::CreateTableStatement>(r)); }
            },
            {
                typeid(Statements::InsertStatement),
                [](const auto& r) { return new Statements::InsertStatement(std::any_cast<Statements::InsertStatement>(r)); }
            },

            {
                typeid(Statements::CreateDbStatement),
                [](const auto& r) { return new Statements::CreateDbStatement(std::any_cast<Statements::CreateDbStatement>(r)); }
            },

            {
                typeid(Statements::DropDbStatement),
                [](const auto& r) { return new Statements::DropDbStatement(std::any_cast<Statements::DropDbStatement>(r)); }
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