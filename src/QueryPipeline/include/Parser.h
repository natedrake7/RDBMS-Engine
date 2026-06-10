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
        class PlanNode;
    }

    class LogicalPlan;
    class Cursor;

    struct CompileValidationScope {
        Dictionary<DataTypes::String, DataType> variables;
    };

    // struct StatementValidationScope {
    //     Dictionary<std::string, table_id_t> tableAliasesDictionary;
    // };

    struct QueryContext {
        CompileValidationScope _scope;
        CompileContext _compileContext;
        DataStructures::PolymorphicArray<Cursor*> cursors;
        Errors::Error status;
        bool hasMore;

        PipelineConstants::ExecutionMode _executionMode;

        QueryContext();
        explicit QueryContext(const Errors::Error& error);
        QueryContext(const QueryContext&) = delete;
        QueryContext& operator=(const QueryContext&) = delete;
        QueryContext(QueryContext&& other) noexcept;
        QueryContext& operator=(QueryContext&& other) noexcept;

        void CreateValidationScope(const Dictionary<DataTypes::String, Variable>& sessionVariables);
        const ::Memory::IAllocator* GetAllocator()const;
    };

    class Parser{
        static void CreateStatements(
            CompileContext& context,
            const std::any &queries,
            const DataTypes::Guid& sessionId
        );
        static void Parse(QueryContext& result, const DataTypes::Guid& sessionId, const std::string& query);
        static LogicalPlan* BuildLogicalPlan(QueryContext& result, Statements::Statement* statement);
        static PhysicalPlan::PlanNode* BuildExecutionPlan(QueryContext& result, LogicalPlan* logicalPlan);
        static void CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, PipelineConstants::cursor_id_t cursorId);

        public:
            Parser();
            ~Parser();

            static Parser& Get()
            {
                static Parser instance;
                return instance;
            }

            static QueryContext StartTransaction(const std::string& query, const DataTypes::Guid& sessionId);

            static void CommitTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
            static void RollbackTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
    };

}
