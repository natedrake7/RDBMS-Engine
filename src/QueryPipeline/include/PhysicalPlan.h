#pragma once
#include "Statements.h"
#include "../../CoreEngine/include/Errors.h"
#include "../../Systemic/include/Headers.h"
#include "../../CoreEngine/include/DataStorage/Row.h"
#include "../../CoreEngine/include/ScanState.h"
#include "../../Systemic/include/DataStructures/PriorityQueue.h"
#include "../../CoreEngine/include/Algorithms/Sort/SortingFunctions.h"
#include "../../Systemic/include/DataStructures/PolymorphicArray.h"

struct MergeElement;

namespace CoreEngine {
    struct SelectionVector;
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
    struct ExecutionResult;

    static void PerformNullJoin(
        const ::Memory::IAllocator* allocator,
        ExecutionResult& result,
        CoreEngine::StorageTypes::RID* outerRow,
        Int numberOfColumns
    );
    static void PerformJoin(
        const ::Memory::IAllocator* allocator,
        ExecutionResult& result,
        const CoreEngine::StorageTypes::RID* outerRow,
        const CoreEngine::StorageTypes::RID* innerRow
    );

    struct VectorBatch{
        CoreEngine::DataVector** _columns;
        Int _numberOfColumns;
        Int _numberOfRows;

        VectorBatch()
            : _columns(nullptr), _numberOfColumns(0), _numberOfRows(0) {}

        void AllocateColumns(const ::Memory::IAllocator* allocator, Int numberOfColumns);
        void SetColumn(CoreEngine::DataVector* columnData, Int columnIndex) const;
    };

    struct ExecutionResult {
        //metadata structures of the query
        Errors::RuntimeStatus status;

        DataStructures::PolymorphicArray<DataTypes::String> displayColumnNames;
        DataStructures::PolymorphicArray<const CoreEngine::StorageTypes::Column*> columns;

        VectorBatch vectorBatch;
        CoreEngine::SelectionVector* selectionVector;
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
        void InsertToTemporaryDatabase(const DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>& rows);
        void InsertPostProjectionResultsToTemporaryDatabase(
            const CoreEngine::ExecutionContext& context,
            ExecutionResult& result,
            DataTypes::RowIdentifier& firstRowId
        );
        [[nodiscard]] ExecutionResult StreamFromTemporaryDatabase(
            const CoreEngine::ExecutionContext& context,
            CoreEngine::ScanState& state
        ) const;
        virtual ExecutionResult Execute(CoreEngine::ExecutionContext& context) = 0;
        virtual void UpdateScanState(const CoreEngine::StorageTypes::RID* rid);

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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalGrantRole final : public PlanNode {
        DataTypes::String username;
        DataTypes::String roleName;
    public:
        explicit PhysicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& roleName);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalCreateDatabase final : public PlanNode {
        DataTypes::String dbName;
    public:
        explicit PhysicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& name);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalUseDatabase final : public PlanNode {
        DataTypes::Guid sessionId;
        Int databaseId;
    public:
        explicit PhysicalUseDatabase(const DataTypes::Guid& sessionId, Int databaseId);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalSchemaCreate final : public PlanNode {
        DataTypes::String schemaName;
        Int databaseId;
    public:
        explicit PhysicalSchemaCreate(const DataTypes::Guid& sessionId, Int databaseId, DataTypes::String& schemaName);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalDropColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::DropColumn* column;
    public:
        PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::DropColumn* column);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalRenameColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::RenameColumn* column;
    public:
        PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::RenameColumn* column);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalAlterColumn final : public PlanNode {
        Statements::DataSource* table;
        Statements::AlterColumn* column;
    public:
        PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::AlterColumn* column);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    class PhysicalIndexScan final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
        bool isClustered;
    public:
        explicit PhysicalIndexScan(Statements::DataSource* table, bool isClustered = false);
        explicit PhysicalIndexScan(Statements::DataSource* table, Expressions::Expression* expression, bool isClustered = false);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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

        inline void ExecuteVectorizedMode(const ExecutionResult& result, const CoreEngine::ExecutionContext& context)const;
        inline void ExecuteRowMode(const ExecutionResult& result, const CoreEngine::ExecutionContext& context)const;

        [[nodiscard]] inline ExecutionResult ExecuteStatement(CoreEngine::ExecutionContext& context) const;
        [[nodiscard]] inline ExecutionResult ExecuteConstantStatement(const CoreEngine::ExecutionContext& context) const;

    public:
        PhysicalProject(
            PlanNode* child,
            DataStructures::PolymorphicArray<Expressions::Expression*>& resultExpressions,
            DataStructures::PolymorphicArray<Headers::ColumnHeader>& columnHeaders
        );
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    class PhysicalFilter final : public PlanNode {
        Expressions::Expression* filter;
        PlanNode* child;

        inline void ExecuteVectorizedMode(const ExecutionResult& result, const CoreEngine::ExecutionContext& context)const;
        inline void ExecuteRowMode(const ExecutionResult& result, const CoreEngine::ExecutionContext& context)const;

    public:
        PhysicalFilter(PlanNode* child, Expressions::Expression* filter);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    class PhysicalTop final : public PlanNode {
        int64_t top;
        PlanNode* child;
    public:
        PhysicalTop(PlanNode* child, BigInt top);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    class PhysicalDistinct final : public PlanNode {
        PlanNode* child;
    public:
        explicit PhysicalDistinct(PlanNode* child);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    class PhysicalOrderBy final : public PlanNode {
        PlanNode* child;
        DataStructures::PolymorphicArray<Statements::OrderColumn*> expressions;

        MergeComparator comparator;
        PriorityQueue<MergeElement, MergeComparator> priorityQueue;

        [[nodiscard]] bool CanBeSortedInMemory(bool canFetchMore) const;
    public:
        PhysicalOrderBy(PlanNode* child, DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
        void UpdateScanState(const CoreEngine::StorageTypes::RID* rid) override;
    };

    /** @} End of Select Processing Classes */

    /**
     * @name Insert and Update Classes
     * Classes that modify data such as insert, update, delete, etc.
     * @{
     */

    class PhysicalInsert final : public PlanNode {
        CoreEngine::StorageTypes::InsertPlan insertPlan;
        DataStructures::PolymorphicArray<Statements::Inserts> fields;

        Statements::DataSource* table;
        PlanNode* child;

        static bool SortInsertsAscending(const Value& lhs, const Value& rhs);

        ExecutionResult InsertFromChild(CoreEngine::StorageTypes::Table* tablePtr, CoreEngine::ExecutionContext& context) const;
        ExecutionResult InsertFromValues(
            CoreEngine::StorageTypes::Table* tablePtr,
            const CoreEngine::ExecutionContext& context
        ) const;
    public:
        PhysicalInsert(
            Statements::DataSource* table,
            DataStructures::PolymorphicArray<Statements::Inserts>& fields,
            PlanNode* child,
            CoreEngine::StorageTypes::InsertPlan& insertPlan
        );
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexScanUpdate final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;
    public:
        PhysicalIndexScanUpdate(Statements::DataSource* table, Expressions::Expression* expression, DataStructures::PolymorphicArray<Expressions::Expression*>& updates);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexSeekUpdate final : public PlanNode {
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;
    public:
        PhysicalIndexSeekUpdate(Statements::DataSource* table, Expressions::Expression* expression, DataStructures::PolymorphicArray<Expressions::Expression*>& updates);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalHeapDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
    public:
        PhysicalHeapDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexScanDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
    public:
        PhysicalIndexScanDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalIndexSeekDelete final : public PlanNode {
        Statements::DataSource* table;
        Expressions::Expression* expression;
        CoreEngine::IndexState state;
    public:
        PhysicalIndexSeekDelete(Statements::DataSource* table, Expressions::Expression* expression);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Insert and Update Classes */

    /**
     * @name Join Classes
     * Classes that perform joins such as nested loop join, merge join, hash join, etc.
     * @{
     */

    // --- Nested Loop Joins ---
    class PhysicalNestedLoopInnerJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        [[nodiscard]] ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalNestedLoopInnerJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* joinCondition
        );
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalNestedLoopLeftJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        [[nodiscard]] ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;

    public:
        PhysicalNestedLoopLeftJoin(
            PlanNode* left,
            PlanNode* right,
            Expressions::Expression* expression
        );
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    // --- Merge Joins ---

    class PhysicalMergeInnerJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalMergeLeftJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalMergeFullJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;
        Expressions::Expression* expression;

        DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
        DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalCrossInnerJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalCrossInnerJoin(PlanNode* left, PlanNode* right);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalCrossLeftJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalCrossLeftJoin(PlanNode* left, PlanNode* right);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    class PhysicalCrossFullJoin final : public PlanNode {
        PlanNode* left;
        PlanNode* right;

        ExecutionResult ExecuteBatchJoin(
            CoreEngine::ExecutionContext& context,
            ExecutionResult& leftResult
        ) const;
    public:
        PhysicalCrossFullJoin(PlanNode* left, PlanNode* right);
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
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
        ExecutionResult Execute(CoreEngine::ExecutionContext& context) override;
    };

    /** @} End of Variable Classes */
}