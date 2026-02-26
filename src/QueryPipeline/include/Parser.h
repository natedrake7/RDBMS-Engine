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

    struct CompileValidationScope {
        Dictionary<std::string, DataType> variables;
    };

    // struct StatementValidationScope {
    //     Dictionary<std::string, table_id_t> tableAliasesDictionary;
    // };

    struct CompileResult {
        CompileValidationScope _scope;
        CompileContext _context;
        DataStructures::PolymorphicArray<Cursor*> cursors;
        Errors::Error status;
        bool hasMore;

        CompileResult();
        explicit CompileResult(const Errors::Error& error);
        CompileResult(const CompileResult&) = delete;
        CompileResult& operator=(const CompileResult&) = delete;
        CompileResult(CompileResult&& other) noexcept;
        CompileResult& operator=(CompileResult&& other) noexcept;

        void CreateValidationScope(const Dictionary<std::string, Variable>& sessionVariables);
        const Memory::Allocator& GetAllocator()const;
    };

    class Parser{
        static void CreateStatements(
            CompileContext& context,
            const std::any &queries,
            const DataTypes::Guid& sessionId
        );
        static void Parse(CompileResult& result, const DataTypes::Guid& sessionId, const std::string& query);
        static LogicalPlan* BuildLogicalPlan(CompileResult& result, Statements::Statement* statement);
        static PhysicalPlan::ExecutionNode* BuildExecutionPlan(CompileResult& result, LogicalPlan* logicalPlan);
        static void CleanUpPostExecutionObjects(const DataTypes::Guid& sessionId, PipelineConstants::cursor_id_t cursorId);

        public:
            Parser();
            ~Parser();

            static Parser& Get()
            {
                static Parser instance;
                return instance;
            }

            static CompileResult StartTransaction(const std::string& query, const DataTypes::Guid& sessionId);

            static void CommitTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
            static void RollbackTransaction(const DataTypes::Guid& sessionId, const Cursor* cursor);
    };

}
