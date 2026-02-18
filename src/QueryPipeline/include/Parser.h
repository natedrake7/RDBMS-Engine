#pragma once
#include <any>
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../Systemic/include/Errors.h"
#include "../../Systemic/include/QueryResult.h"
#include <cstdint>
#include <string>

#include "DatabaseConstants.h"
#include "../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "CompileContext.h"

namespace QueryPipeline{

    namespace Statements {
        struct Statement;
    }

    namespace PhysicalPlan {
        class ExecutionNode;
    }

    class LogicalPlan;
    class Cursor;

    struct ParserValidationScope {
        Dictionary<std::string, DataType> variables;
    };

    // struct StatementValidationScope {
    //     Dictionary<std::string, table_id_t> tableAliasesDictionary;
    // };

    struct ParserResult {
        ParserValidationScope validationScope;
        CompileContext _compileContext;
        DataStructures::PolymorphicArray<QueryResult> rows;
        DataStructures::PolymorphicArray<std::string> columns;
        std::vector<Cursor*> cursors;

        Errors::Error status;

        bool hasMore;

        ParserResult() {
            this->hasMore = false;
        }

        void CreateValidationScope(const Dictionary<std::string, Variable>& sessionVariables);

        explicit ParserResult(const Errors::Error& error);
        ParserResult(const ParserResult&) = delete;
        ParserResult& operator=(const ParserResult&) = delete;
        ParserResult(ParserResult&& other) noexcept;
        ParserResult& operator=(ParserResult&& other) noexcept;
    };


    class Parser{
        static void CreateStatements(
            CompileContext& context,
            const std::any &queries,
            const DataTypes::Guid& sessionId
        );
        static void ClearQuery(const std::vector<Statements::Statement*>& statements);

        static QueryPipeline::CompileContext Parse(ParserResult& result, const DataTypes::Guid& sessionId, const std::string& query);
        static LogicalPlan* BuildLogicalPlan(ParserResult& result, Statements::Statement* statement);
        static PhysicalPlan::ExecutionNode* BuildExecutionPlan(ParserResult& result, LogicalPlan* logicalPlan);
        static void CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, PipelineConstants::cursor_id_t cursorId);

        public:
            Parser();
            ~Parser();

            static Parser& Get()
            {
                static Parser instance;
                return instance;
            }

            static ParserResult StartTransaction(const std::string& query, const DataTypes::Guid& sessionId);

            static ParserResult Execute(Cursor* cursor);

            static void CommitTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
            static void RollbackTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
    };

}
