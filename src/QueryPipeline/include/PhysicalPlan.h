#pragma once
#include <vector>
#include <string>

#include "Statements.h"
#include "../../Systemic/include/Errors.h"
#include "../../Systemic/include/QueryResult.h"
#include "../../Systemic/include/Headers.h"
#include "../../CoreEngine/include/DataStorage/Row.h"
#include "../../CoreEngine/include/ScanState.h"
#include "../../Systemic/include/DataStructures/PriorityQueue.h"
#include "../../CoreEngine/include/Algorithms/Sort/SortingFunctions.h"
#include "../../Systemic/include/DataStructures/PolymorphicArray.h"

struct MergeElement;

namespace CoreEngine {
    class ExecutionContext;
    class SystemCatalog;
}

namespace Network {
    struct Session;
}

namespace QueryPipeline {
    class LogicalPlan;
}

namespace QueryPipeline::PhysicalPlan {
    struct ExecutionResult {
        DataStructures::PolymorphicArray<DataTypes::String> displayColumnNames;
        DataStructures::PolymorphicArray<const CoreEngine::StorageTypes::Column*> columns;
        DataStructures::PolymorphicArray<Pages::RowReference> rows;
        DataStructures::PolymorphicArray<QueryResult> results;

        Errors::RuntimeStatus status;

        bool canFetchMore;

        explicit ExecutionResult(const CoreEngine::ExecutionContext& context);
        ExecutionResult(const Errors::RuntimeError& code, const DataTypes::String& message);
        ExecutionResult(
            const Errors::RuntimeError& code,
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        );

        ExecutionResult(ExecutionResult&& other) noexcept;
        ExecutionResult& operator=(ExecutionResult&& other) noexcept;

        ~ExecutionResult();

        [[nodiscard]] bool IsOk() const;
    };

    class PlanNode {
    protected:
        DataTypes::Guid sessionId;

        CoreEngine::SystemCatalog* catalog;
        Network::Server* server;

        const Network::Session* session;

        Int temporaryTableId;

    public:
        PlanNode();
        explicit PlanNode(const DataTypes::Guid& currentSessionId);
        virtual ~PlanNode() = default;
        void InsertToTemporaryDatabase(const DataStructures::PolymorphicArray<Pages::RowReference>& rows);
        void InsertPostProjectionResultsToTemporaryDatabase(
            const CoreEngine::ExecutionContext& context,
            ExecutionResult& result,
            DataTypes::RowIdentifier& firstRowId
        );
        [[nodiscard]] ExecutionResult StreamFromTemporaryDatabase(
            const CoreEngine::ExecutionContext& context,
            CoreEngine::ScanState& state
        ) const;
        virtual ExecutionResult Execute(const CoreEngine::ExecutionContext& context) = 0;
        virtual void UpdateScanState(const DataTypes::RowIdentifier& rowId);

        [[nodiscard]] bool UsesExternalStorage() const;
    };

    /**
     * @name Catalog Altering Classes
     * Classes that alter system catalog such as creating users, databases, schemas, etc.
     * @{
     */

    class PhysicalCreateUser final : public PlanNode {
        DataTypes::String username;
        DataTypes::String password;
        DataTypes::String roleName;
    public:
        explicit PhysicalCreateUser(DataTypes::String& username, DataTypes::String& password, DataTypes::String& role);
        ~PhysicalCreateUser() override = default;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalGrantRole final : public PlanNode {
        DataTypes::String username;
        DataTypes::String roleName;
    public:
        explicit PhysicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& roleName);
        ~PhysicalGrantRole() override = default;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalCreateDatabase final : public PlanNode {
        DataTypes::String dbName;
    public:
        explicit PhysicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& name);
        ~PhysicalCreateDatabase() override = default;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalUseDatabase final : public PlanNode {
        DataTypes::Guid sessionId;
        Int databaseId;
    public:
        explicit PhysicalUseDatabase(const DataTypes::Guid& sessionId, Int databaseId);
        ~PhysicalUseDatabase() override = default;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalSchemaCreate final : public PlanNode {
        DataTypes::String schemaName;
        Int databaseId;
    public:
        explicit PhysicalSchemaCreate(const DataTypes::Guid& sessionId, Int databaseId, DataTypes::String& schemaName);
        ~PhysicalSchemaCreate() override = default;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalTableCreate final : public PlanNode {
        Statements::DataSource* table;
        DataTypes::String constraintName;
        DataStructures::PolymorphicArray<Statements::NewColumn*> columns;
        Headers::Index primaryKey;
    public:
        PhysicalTableCreate(
            const DataTypes::Guid& sessionId,
            Statements::DataSource* table,
            DataStructures::PolymorphicArray<Statements::NewColumn*>& columns,
            const Headers::Index& primaryKey,
            DataTypes::String& constraintName
        );
        ~PhysicalTableCreate() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexCreate final : public PlanNode {
        Statements::DataSource* table;
        DataTypes::String constraintName;
        DataStructures::PolymorphicArray<column_index_t> columns;
    public:
        PhysicalIndexCreate(
            const DataTypes::Guid& sessionId,
            Statements::DataSource* table,
            DataTypes::String& constraintName,
            DataStructures::PolymorphicArray<column_index_t>& columns
        );
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Catalog Altering Classes */

    /**
     * @name Table Alter Classes
     * Classes that alter tables such as add column, drop column, rename column, etc.
     * @{
     */

    class PhysicalAddColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::NewColumn* column;
    public:
        PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::NewColumn* column);
        ~PhysicalAddColumn() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalDropColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::DropColumn* column;
    public:
        PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::DropColumn* column);
        ~PhysicalDropColumn() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalRenameColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::RenameColumn* column;
    public:
        PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::RenameColumn* column);
        ~PhysicalRenameColumn() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalAlterColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::AlterColumn* column;
    public:
        PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::AlterColumn* column);
        ~PhysicalAlterColumn() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Table Alter Classes */

    /**
     * @name Table Scan Classes
     * Classes that scan tables such as table scan, index scan, index seek, etc.
     * @{
     */

    class PhysicalTableScan final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::ScanState state;
    public:
        explicit PhysicalTableScan(Statements::DataSource* table, Expressions::Expression* expression);
        ~PhysicalTableScan() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalIndexScan final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
        bool isClustered;
    public:
        explicit PhysicalIndexScan(Statements::DataSource* table, bool isClustered = false);
        explicit PhysicalIndexScan(Statements::DataSource* table, Expressions::Expression* expression, bool isClustered = false);
        ~PhysicalIndexScan() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalIndexSeek final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        DataTypes::Indexing::Key key;
    public:
        explicit PhysicalIndexSeek(
            Statements::DataSource* table,
            DataTypes::Indexing::Key& key,
            Expressions::Expression* expression
        );
        ~PhysicalIndexSeek() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexSeekRange final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        DataTypes::Indexing::Key minKey;
        DataTypes::Indexing::Key maxKey;
    public:
        explicit PhysicalIndexSeekRange(
            Statements::DataSource* table,
            DataTypes::Indexing::Key& minKey,
            DataTypes::Indexing::Key& maxKey,
            Expressions::Expression* expression
        );
        ~PhysicalIndexSeekRange() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Table Scan Classes */

    /**
     * @name Select Processing Classes
     * Classes that process data retrieved from scans such as projection, filtering, top, distinct, etc.
     * @{
     */

    class PhysicalProject final : public PlanNode {
        DataStructures::PolymorphicArray<Expressions::Expression*> resultExpressions;
        DataStructures::PolymorphicArray<Headers::ColumnHeader> columnHeaders;
        PlanNode* child;

        [[nodiscard]] inline ExecutionResult ExecuteStatement(const CoreEngine::ExecutionContext& context) const;
        [[nodiscard]] inline ExecutionResult ExecuteConstantStatement(const CoreEngine::ExecutionContext& context) const;
    public:
        PhysicalProject(
            PlanNode* child,
            DataStructures::PolymorphicArray<Expressions::Expression*>& resultExpressions,
            DataStructures::PolymorphicArray<Headers::ColumnHeader>& columnHeaders
        );
        ~PhysicalProject() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalFilter final : public PlanNode {
        Expressions::Expression* filter;
        PlanNode* child;
    public:
        PhysicalFilter(PlanNode* child, Expressions::Expression* filter);
        ~PhysicalFilter() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalTop final : public PlanNode {
        int64_t top;
        PlanNode* child;
    public:
        PhysicalTop(PlanNode* child, BigInt top);
        ~PhysicalTop() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalDistinct final : public PlanNode {
        PlanNode* child;
    public:
        explicit PhysicalDistinct(PlanNode* child);
        ~PhysicalDistinct() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    class PhysicalOrderBy final : public PlanNode {
        PlanNode* child;
        DataStructures::PolymorphicArray<Statements::OrderColumn*> expressions;

        MergeComparator comparator;
        PriorityQueue<MergeElement, MergeComparator> priorityQueue;

        [[nodiscard]] bool CanBeSortedInMemory(bool canFetchMore) const;
    public:
        PhysicalOrderBy(PlanNode* child, DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions);
        ~PhysicalOrderBy() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
    };

    /** @} End of Select Processing Classes */

    /**
     * @name Insert and Update Classes
     * Classes that modify data such as insert, update, delete, etc.
     * @{
     */

    class PhysicalInsert final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Statements::Inserts> fields;

        PlanNode* child;
        DataStructures::PolymorphicArray<column_index_t> columnsIndices;

        static bool SortInsertsAscending(const Value& lhs, const Value& rhs);

        DataStructures::PolymorphicArray<Value> ConvertExpressionsToValues(
            const CoreEngine::ExecutionContext& context,
            Int index
        ) const;
        ExecutionResult InsertFromChild(CoreEngine::StorageTypes::Table* tablePtr, const CoreEngine::ExecutionContext& context) const;
        ExecutionResult InsertFromFields(CoreEngine::StorageTypes::Table* tablePtr, const CoreEngine::ExecutionContext& context) const;
    public:
        PhysicalInsert(
            Statements::DataSource* table,
            DataStructures::PolymorphicArray<Statements::Inserts>& fields,
            PlanNode* child,
            DataStructures::PolymorphicArray<column_index_t>& columnsIndices
        );
        ~PhysicalInsert() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalHeapUpdate final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;
    public:
        PhysicalHeapUpdate(
            Statements::DataSource* table,
            Expressions::Expression* expression,
            DataStructures::PolymorphicArray<Expressions::Expression*>& updates
        );
        ~PhysicalHeapUpdate() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexScanUpdate final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;
    public:
        PhysicalIndexScanUpdate(Statements::DataSource* table, Expressions::Expression* expression, DataStructures::PolymorphicArray<Expressions::Expression*>& updates);
        ~PhysicalIndexScanUpdate() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexSeekUpdate final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;
    public:
        PhysicalIndexSeekUpdate(Statements::DataSource* table, Expressions::Expression* expression, DataStructures::PolymorphicArray<Expressions::Expression*>& updates);
        ~PhysicalIndexSeekUpdate() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalHeapDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
    public:
        PhysicalHeapDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ~PhysicalHeapDelete() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexScanDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
    public:
        PhysicalIndexScanDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ~PhysicalIndexScanDelete() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexSeekDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
    public:
        PhysicalIndexSeekDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ~PhysicalIndexSeekDelete() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Insert and Update Classes */

    /**
     * @name Join Classes
     * Classes that perform joins such as nested loop join, merge join, hash join, etc.
     * @{
     */

    class PhysicalNestedLoopInnerJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        [[nodiscard]] ExecutionResult ExecuteBatchJoin(
            const CoreEngine::ExecutionContext& context,
            const ExecutionResult& leftResult
        ) const;
    public:
        PhysicalNestedLoopInnerJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* joinCondition
        );
        ~PhysicalNestedLoopInnerJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalMergeInnerJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            const CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalMergeInnerJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* expression,
            DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
            DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
        );
        ~PhysicalMergeInnerJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalMergeLeftJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            const CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalMergeLeftJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* expression,
            DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
            DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
        );
        ~PhysicalMergeLeftJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalMergeFullJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            const CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalMergeFullJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* expression,
            DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
            DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
        );
        ~PhysicalMergeFullJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalNestedLoopLeftJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;
    public:
        PhysicalNestedLoopLeftJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* expression
        );
        ~PhysicalNestedLoopLeftJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalNestedLoopFullJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* joinCondition;
    public:
        PhysicalNestedLoopFullJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* joinCondition
        );
        ~PhysicalNestedLoopFullJoin() override;
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Join Classes */

    /**
     * @name Variable Classes
     * Classes that declare and update session variables
     * @{
     */

    class PhysicalDeclareVariable final : public PlanNode {
        Variable variable;
        Expressions::Expression* expression;
    public:
        explicit PhysicalDeclareVariable(const DataTypes::Guid& currentSessionId, Variable& variable, Expressions::Expression* expression);
        ExecutionResult Execute(const CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Variable Classes */
}