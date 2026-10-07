#pragma once
#include <Systemic/DataStructures/Dictionary.h>
#include <CoreEngine/Errors.h>
#include <string>

#include <QueryPipeline/DatabaseConstants.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <QueryPipeline/CompileContext.h>
#include <QueryPipeline/ReferencedColumns.h>
#include <CoreEngine/Contexts/ExecutionContext.h>

namespace Network{
    struct Session;
}

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
        Dictionary<DataTypes::StringView, DataType> variables;
    };

    struct QueryContext {
        ReferencedColumns _referencedColumns;
        CompileValidationScope _scope;
        CompileContext _compileContext;
        CoreEngine::ExecutionContext _executionContext;
        DataStructures::PolymorphicArray<Cursor*> cursors;
        Errors::ParserStatus status;
        const Network::Session* _session;

        UnsignedSmallInt _virtualId;
        bool hasMore;

        QueryContext();
        explicit QueryContext(Errors::ParserStatus&  error);

        QueryContext(const QueryContext&) = delete;
        QueryContext& operator=(const QueryContext&) = delete;

        QueryContext(QueryContext&& other) noexcept;
        QueryContext& operator=(QueryContext&& other) noexcept;

        void CreateValidationScope(const Network::Session* session);
        const ::Memory::IAllocator* GetAllocator()const;

        [[nodiscard]] UnsignedSmallInt NextVirtualId();

        void Release()const;
    };

    class Parser{
        static void Parse(QueryContext& result, session_id_t sessionId, const std::string& query);
        static LogicalPlan* BuildLogicalPlan(QueryContext& result, Statements::Statement* statement);
        static PhysicalPlan::PlanNode* BuildExecutionPlan(QueryContext& result, LogicalPlan* logicalPlan);
        static void CleanUpPostExecutionObjects(session_id_t sessionId, PipelineConstants::cursor_id_t cursorId);

        public:
            static Parser& Get(){
                static Parser instance;
                return instance;
            }

            static QueryContext StartTransaction(const std::string& query, session_id_t sessionId);

            static void CommitTransaction(session_id_t sessionId, const Cursor* cursor);
            static void RollbackTransaction(session_id_t sessionId, const Cursor* cursor);
    };

}
