#include "../include/LogicalPlan.h"
#include "../../CoreEngine/include/Managers/StatisticsManager.h"
#include "../include/Optimizer.h"
#include "../include/Statements.h"
#include "../../CoreEngine/include/SystemDatabases/SystemCatalog.h"

#include <utility>

#include "DatabaseConstants.h"
#include "../../CoreEngine/include/Contexts/OutputSchema.h"
#include "Parser.h"
#include "../../CoreEngine/include/DataStorage/ColumnMaterializationInfo.h"
#include "../../CoreEngine/include/Evaluators/Kernels/Vectorized/Vectorized.JumpTables.h"
#include "../../CoreEngine/include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"


namespace QueryPipeline {
    LogicalPlan::LogicalPlan(const DataTypes::Guid &sessionId, const Int databaseId)
        : sessionId(sessionId), databaseId(databaseId) {}

    LogicalPlan::LogicalPlan(const DataTypes::Guid &sessionId)
        : sessionId(sessionId), databaseId(INVALID_DATABASE_ID) {}

    LogicalPlan::LogicalPlan()
        : sessionId(DataTypes::Guid()), databaseId(INVALID_DATABASE_ID) {}

    LogicalMaterialize::LogicalMaterialize(
        LogicalPlan* child,
        const UnsignedSmallInt slotIndex
    ):  child(child), _slotIndex(slotIndex){}

    PhysicalPlan::PlanNode* LogicalMaterialize::ToPhysical(QueryContext& context){
        const auto* allocator = context._compileContext.GetAllocator();
        const auto numOfColumns = context._referencedColumns.GetSlotCount(this->_slotIndex);

        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ColumnMaterializationInfo> materializationInfo(
            allocator,
            numOfColumns
        );

        auto* childPhysical = this->child->ToPhysical(context);

        context._referencedColumns.ForEachColumn(this->_slotIndex, [&](
            const column_index_t ordinalPosition,
            const DataType dataType
        ){
            materializationInfo.Push(
            CoreEngine::StorageTypes::ColumnMaterializationInfo(
                    CoreEngine::VectorizedKernels::JumpTables::GetMaterializationFunction(dataType),
                    ordinalPosition,
                    dataType
                )
            );
        });

        return context._compileContext.Allocate<PhysicalPlan::PhysicalMaterialize>(
            materializationInfo, childPhysical,
            childPhysical->GetSchema(), this->_slotIndex
        );
    }

    LogicalDeclareVariable::LogicalDeclareVariable(const DataTypes::Guid &sessionId, Variable& variable, Expressions::Expression* expression)
        : LogicalPlan(sessionId), variable(std::move(variable)), expression(expression) {}

    PhysicalPlan::PlanNode* LogicalDeclareVariable::ToPhysical(QueryContext& context) {
        Expressions::BindExpressionRowKernel(this->expression);
        return context._compileContext.Allocate<PhysicalPlan::PhysicalDeclareVariable>(this->sessionId, this->variable, this->expression);
    }

    LogicalCreateUser::LogicalCreateUser(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& password, DataTypes::String& role)
        : LogicalPlan(sessionId), username(std::move(username)), password(std::move(password)), role(std::move(role)) {}

    PhysicalPlan::PlanNode* LogicalCreateUser::ToPhysical(QueryContext& context) {
        return context._compileContext.Allocate<PhysicalPlan::PhysicalCreateUser>(this->username, this->password, this->role);
    }

    LogicalGrantRole::LogicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& role)
        : LogicalPlan(sessionId), username(std::move(username)), role(std::move(role)) {}

    PhysicalPlan::PlanNode* LogicalGrantRole::ToPhysical(QueryContext& context) {
        return context._compileContext.Allocate<PhysicalPlan::PhysicalGrantRole>(this->sessionId, this->username, this->role);
    }

    LogicalCreateDatabase::LogicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& dbName)
        : LogicalPlan(sessionId), dbName(std::move(dbName)) {}

    PhysicalPlan::PhysicalCreateDatabase* LogicalCreateDatabase::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalCreateDatabase>(this->sessionId, this->dbName);
    }

    LogicalUseDatabase::LogicalUseDatabase(const DataTypes::Guid &sessionId, const Int databaseId)
        : databaseId(databaseId), sessionId(sessionId) {}

    PhysicalPlan::PhysicalUseDatabase* LogicalUseDatabase::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalUseDatabase>(this->sessionId, this->databaseId);
    }

    LogicalProject::LogicalProject(
        LogicalPlan *child,
        DataStructures::PolymorphicArray<Expressions::Expression*>& projections,
        const UnsignedSmallInt slotCount
    ):   child(child), _projections(std::move(projections)),
        _slotCount(slotCount) {}

    PhysicalPlan::PhysicalProject* LogicalProject::ToPhysical(QueryContext& context){
        auto* childPhysical = this->child != nullptr
            ? this->child->ToPhysical(context)
            : nullptr;

        auto* childSchema = this->child != nullptr
            ? childPhysical->GetSchema()
            : nullptr;

        auto* outputSchema = context._compileContext.GetAllocator()->Allocate<CoreEngine::OutputSchema>(
            context._compileContext.GetAllocator(),
            this->_projections.Size()
        );

        for (auto* expression : this->_projections){
            Expressions::BindAndResolveExpressionKernel(expression, childSchema);

            if (expression->IsColumn()){
                const auto* columnExpression = expression->AsColumn();
                outputSchema->_columns.Push(
                CoreEngine::SchemaColumn::Base(
                        columnExpression->_slotIndex,
                        columnExpression->ordinalPosition,
                        columnExpression->returnType
                    )
                );

                continue;
            }

            outputSchema->_columns.Push(
            CoreEngine::SchemaColumn::Computed(
                    context.NextVirtualId(),
                    Expressions::GetExpressionReturnType(expression)
                )
            );
        }

        return context._compileContext.Allocate<PhysicalPlan::PhysicalProject>(
            childPhysical,
            this->_projections,
            outputSchema,
            this->_slotCount
        );
    }

    bool LogicalTableScan::HasPredicate() const{
        return this->expression != nullptr;
    }

    DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo> LogicalTableScan::BuildFilterColumns(
        const QueryContext& context,
        const CoreEngine::OutputSchema* schema
    ) const{
        const auto* allocator = context._compileContext.GetAllocator();
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo> filterColumnsInfo(allocator);

        if (!this->HasPredicate())
            return filterColumnsInfo;

        UnsignedBigInt seenColumns[ReferencedColumns::WORDS] = {};
        const auto slotIndex = this->table->_slotIndex;
        Expressions::ForEachColumnReference(this->expression, [&](const Expressions::ColumnExpression* columnExpression)
        {
            if (columnExpression->_slotIndex != slotIndex)
                return;

            const auto ordinalPosition = columnExpression->ordinalPosition;
            auto* __restrict__ word = &seenColumns[ordinalPosition >> 6];
            const auto bit = ordinalPosition & 63;

            if (PackedWord<UnsignedBigInt>::GetBit(*word, bit))
                return;

            PackedWord<UnsignedBigInt>::SetBit(word, bit, true);
            const auto position = schema->IndexOf(
                CoreEngine::ColumnIdentity::Base(slotIndex, ordinalPosition)
            );

            if (position == INVALID_COLUMN_INDEX)
                return;

            const auto type = schema->_columns[position]._type;
            filterColumnsInfo.Push(
            CoreEngine::StorageTypes::FilterColumnInfo(
                    CoreEngine::VectorizedKernels::JumpTables::GetPageMaterializationFunction(type),
                    ordinalPosition,
                    position,
                    type
                )
            );
        });


        return filterColumnsInfo;
    }

    LogicalTableScan::LogicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
        : table(table), expression(expression) {}

    PhysicalPlan::PlanNode* LogicalTableScan::ToPhysical(QueryContext& context){
        const auto* allocator = context._compileContext.GetAllocator();

        const auto slotIndex = this->table->_slotIndex;
        const auto numOfColumns = context._referencedColumns.GetSlotCount(this->table->_slotIndex);

        auto* outputSchema = allocator->Allocate<CoreEngine::OutputSchema>(allocator, numOfColumns);

        context._referencedColumns.ForEachColumn(slotIndex, [&](
            const column_index_t ordinalPosition,
            const DataType dataType
        ){
            outputSchema->_columns.Push(
                CoreEngine::SchemaColumn::Base(slotIndex, ordinalPosition, dataType)
            );
        });

        auto filterColumnsInfo = this->BuildFilterColumns(context, outputSchema);

        if (this->HasPredicate())
            Expressions::BindAndResolveExpressionKernel(this->expression, outputSchema);

        auto indexes = CoreEngine::SystemCatalog::Get().SelectIndexes(allocator, this->table->_tableId);

        // If no indexes are available, use heap scan
        if (indexes.Empty()){
            Expressions::BindAndResolveExpressionKernel(this->expression, outputSchema);
            return context._compileContext.Allocate<PhysicalPlan::PhysicalTableScan>(this->table, outputSchema, this->expression);
        }

        // If no filter expression, choose the best index for scanning
        const auto& firstIndex = indexes[0];
        if (!this->HasPredicate()) {
            if (firstIndex.isClustered)
                return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexScan>(
                    this->table, outputSchema,
                    filterColumnsInfo, true
                );

            // Otherwise use heap scan
            return context._compileContext.Allocate<PhysicalPlan::PhysicalTableScan>(this->table, outputSchema, this->expression);
        }

        const auto tableStats = CoreEngine::StatisticsManager::Get().GetTableStatistics(this->table->_tableId);

        // No table stats yet, or small table
        if (tableStats.tableId == INVALID_TABLE_ID || tableStats.rowCount < PipelineConstants::SMALL_TABLE){
            Expressions::BindAndResolveExpressionKernel(this->expression, outputSchema);
            return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexScan>(
                this->table, outputSchema, filterColumnsInfo,
                this->expression, firstIndex.isClustered
            );
        }

        Optimizer optimizer(context);
        // else use optimizer to choose index seek/scan
        auto result = optimizer.PerformIndexAnalysis(indexes, this->expression, tableStats);
        
        Expressions::BindAndResolveExpressionKernel(this->expression, outputSchema);

        if (result.hasRange)
            return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexSeekRange>(
                this->table, outputSchema, result.start, result.end,
                filterColumnsInfo, result.remainingPredicate
            );

        // scan the first index
        if (!result.canSeek)
            return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexScan>(
                this->table, outputSchema,
                filterColumnsInfo, result.remainingPredicate,
                indexes[0].isClustered
            );

        return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexSeek>(
            this->table, outputSchema, result.start,
            filterColumnsInfo, result.remainingPredicate
        );
    }

    PhysicalPlan::PlanNode* LogicalJoin::CreateInnerJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    ){
        switch (analysis.algorithm) {
            case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
            case PipelineConstants::JoinAlgorithm::HashJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalNestedLoopInnerJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate
                );
            case PipelineConstants::JoinAlgorithm::MergeJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalMergeInnerJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate,
                    analysis.leftKeyColumns,
                    analysis.rightKeyColumns
                );
            case PipelineConstants::JoinAlgorithm::CrossJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalCrossInnerJoin>(
                    leftPlan,
                    rightPlan
                );
        }

        throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
    }

    PhysicalPlan::PlanNode* LogicalJoin::CreateLeftJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    ){
        switch (analysis.algorithm) {
            case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
            case PipelineConstants::JoinAlgorithm::HashJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalNestedLoopLeftJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate
                );
            case PipelineConstants::JoinAlgorithm::MergeJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalMergeLeftJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate,
                    analysis.leftKeyColumns,
                    analysis.rightKeyColumns
                );
            case PipelineConstants::JoinAlgorithm::CrossJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalCrossLeftJoin>(
                    leftPlan,
                    rightPlan
                );
        }

        throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
    }

    PhysicalPlan::PlanNode* LogicalJoin::CreateRightJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    ){
        switch (analysis.algorithm) {
            case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
            case PipelineConstants::JoinAlgorithm::HashJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalNestedLoopLeftJoin>(
                    rightPlan,
                    leftPlan,
                    analysis.remainingPredicate
                );
            case PipelineConstants::JoinAlgorithm::MergeJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalMergeLeftJoin>(
                    rightPlan,
                    leftPlan,
                    analysis.remainingPredicate,
                    analysis.leftKeyColumns,
                    analysis.rightKeyColumns
                );
            case PipelineConstants::JoinAlgorithm::CrossJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalCrossLeftJoin>(
                    rightPlan,
                    leftPlan
                );
        }

        throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
    }

    PhysicalPlan::PlanNode* LogicalJoin::CreateFullJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    ){
        switch (analysis.algorithm) {
            case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
            case PipelineConstants::JoinAlgorithm::HashJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalNestedLoopFullJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate
                );
            case PipelineConstants::JoinAlgorithm::MergeJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalMergeFullJoin>(
                    leftPlan,
                    rightPlan,
                    analysis.remainingPredicate,
                    analysis.leftKeyColumns,
                    analysis.rightKeyColumns
                );
            case PipelineConstants::JoinAlgorithm::CrossJoin:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalCrossFullJoin>(
                    leftPlan,
                    rightPlan
                );
        }

        throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
    }

    LogicalJoin::LogicalJoin(
        LogicalPlan *left,
        LogicalPlan *right,
        Expressions::Expression *condition,
        const JoinType type,
        const Int leftTableId,
        const Int rightTableId
    ) : leftTableId(leftTableId), rightTableId(rightTableId),
        left(left), right(right),
        condition(condition), type(type) {}

    PhysicalPlan::PlanNode* LogicalJoin::ToPhysical(QueryContext& context){
        const Optimizer optimizer(context);

        auto analysisResult = optimizer.ChooseJoinAlgorithm(
            this->leftTableId,
            this->rightTableId,
            this->condition
        );

        auto* leftPhysical = this->left->ToPhysical(context);
        auto* rightPhysical = this->right->ToPhysical(context);

        const auto* leftSchema = leftPhysical->GetSchema();
        const auto* rightSchema = rightPhysical->GetSchema();

        const auto schemaSize = leftSchema->_columns.Size() + rightSchema->_columns.Size();

        auto* schema = context._compileContext.GetAllocator()->Allocate<CoreEngine::OutputSchema>(context.GetAllocator(), schemaSize);

        for (const auto& column : leftSchema->_columns)
            schema->_columns.Push(column);

        for (const auto& column : rightSchema->_columns)
            schema->_columns.Push(column);

        Expressions::BindAndResolveExpressionKernel(this->condition, schema);

        switch (this->type) {
            case JoinType::Inner:
                return this->CreateInnerJoinPhysicalPlan(context, analysisResult, leftPhysical, rightPhysical);
            case JoinType::Left:
                return this->CreateLeftJoinPhysicalPlan(context, analysisResult, leftPhysical, rightPhysical);
            case JoinType::Right:
                return this->CreateRightJoinPhysicalPlan(context, analysisResult, leftPhysical, rightPhysical);
            case JoinType::Full:
                return this->CreateFullJoinPhysicalPlan(context, analysisResult, leftPhysical, rightPhysical);
            default:
                throw std::runtime_error("Unknown JoinType");
        }
    }

    LogicalFilter::LogicalFilter(
        LogicalPlan* child,
        Expressions::Expression* filter,
        const UnsignedSmallInt slotCount
    ): child(child), filter(filter), _slotCount(slotCount) {}

    PhysicalPlan::PhysicalFilter* LogicalFilter::ToPhysical(QueryContext& context){
        auto* childPhysical = this->child->ToPhysical(context);
        auto* childSchema = childPhysical->GetSchema();

        Expressions::BindAndResolveExpressionKernel(this->filter, childSchema);

        return context._compileContext.Allocate<PhysicalPlan::PhysicalFilter>(childPhysical, this->filter, this->_slotCount);
    }

    LogicalOrder::LogicalOrder(LogicalPlan *child, DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions)
        : child(child), expressions(std::move(expressions)) {}

    PhysicalPlan::PlanNode* LogicalOrder::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalOrderBy>(this->child->ToPhysical(context), this->expressions);
    }

    LogicalTop::LogicalTop(LogicalPlan *child, const BigInt top)
        : child(child), top(top) {}

    PhysicalPlan::PhysicalTop* LogicalTop::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalTop>(this->child->ToPhysical(context), this->top);
    }

    LogicalDistinct::LogicalDistinct(LogicalPlan *child)
        : child(child) {}

    PhysicalPlan::PhysicalDistinct* LogicalDistinct::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalDistinct>(this->child->ToPhysical(context));
    }

    LogicalInsert::LogicalInsert(
        Statements::DataSource* table,
        DataStructures::PolymorphicArray<Statements::Inserts> &fields,
        LogicalPlan* child,
        CoreEngine::StorageTypes::InsertPlan& insertPlan
    ) : table(table), fields(std::move(fields)), child(child), insertPlan(std::move(insertPlan)) {}

    PhysicalPlan::PhysicalInsert* LogicalInsert::ToPhysical(QueryContext& context){
        auto* physicalSelect = this->child != nullptr
            ? this->child->ToPhysical(context)
            : nullptr;

        for (auto& [values] : this->fields){
            for (const auto& expression : values)
                Expressions::BindExpressionRowKernel(expression);
        }

        return context._compileContext.Allocate<PhysicalPlan::PhysicalInsert>(this->table, this->fields, physicalSelect, this->insertPlan);
    }

    LogicalSchemaCreate::LogicalSchemaCreate(const DataTypes::Guid& sessionId, const Int databaseId, DataTypes::String& schemaName)
        : LogicalPlan(sessionId), schemaName(std::move(schemaName)), databaseId(databaseId) {}

    PhysicalPlan::PhysicalSchemaCreate* LogicalSchemaCreate::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalSchemaCreate>(this->sessionId, this->databaseId, this->schemaName);
    }

    LogicalDelete::LogicalDelete(Statements::DataSource *table, Expressions::Expression *expression)
        : table(table), expression(expression) {}

    PhysicalPlan::PlanNode* LogicalDelete::ToPhysical(QueryContext& context){
        const auto indexes = CoreEngine::SystemCatalog::Get().SelectIndexes(context._compileContext.GetAllocator(), this->table->_tableId);

        // If no indexes are available, heap scan
        if (indexes.Empty())
            return context._compileContext.Allocate<PhysicalPlan::PhysicalHeapDelete>(this->table, this->expression);

        // If expression is complex, defer from index seek
        const bool canIndexSeek = expression != nullptr;

        for (const auto& index: indexes) {
            const auto indexHeader = CoreEngine::SystemCatalog::Get().SelectIndexById(context._compileContext.GetAllocator(), index.id);

            if (canIndexSeek) {
                for (const auto& column: index.columns) {
                    break;
                }
            }

            // Find the first non clustered and use it
            return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexScanDelete>(this->table, this->expression);
        }

        return context._compileContext.Allocate<PhysicalPlan::PhysicalHeapDelete>(this->table, this->expression);
    }

    LogicalUpdate::LogicalUpdate(
        Statements::DataSource *table,
        DataStructures::PolymorphicArray<Expressions::Expression*>& updates,
        Expressions::Expression *expression
    ) : table(table), updates(std::move(updates)), expression(expression) {}

    PhysicalPlan::PlanNode* LogicalUpdate::ToPhysical(QueryContext& context){
        const auto indexes = CoreEngine::SystemCatalog::Get().SelectIndexes(context._compileContext.GetAllocator(), this->table->_tableId);

        // If no indexes are available, heap scan
        if (indexes.Empty())
            return context._compileContext.Allocate<PhysicalPlan::PhysicalHeapUpdate>(this->table, this->expression, this->updates);

        // If expression is complex, defer from index seek
        const bool canIndexSeek = expression != nullptr;

        HashSet<column_index_t> expressionColumns;

        for (const auto& index: indexes) {
            const auto indexHeader = CoreEngine::SystemCatalog::Get().SelectIndexById(context._compileContext.GetAllocator(), index.id);

            if (canIndexSeek) {
                for (const auto& column: indexHeader.columns) {
                    if (expressionColumns.Contains(column.ordinalPosition))
                        return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexSeekUpdate>(this->table, this->expression, this->updates);

                    break;
                }
            }

            // Find the first non clustered and use it
            return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexScanUpdate>(this->table, this->expression, this->updates);
        }

        return context._compileContext.Allocate<PhysicalPlan::PhysicalHeapUpdate>(this->table, this->expression, this->updates);
    }

    LogicalTableCreate::LogicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        DataStructures::PolymorphicArray<Statements::NewColumn*>& columns,
        DataStructures::PolymorphicArray<column_index_t> primaryKey,
        DataTypes::String& constraintName
    ) : LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)),
        columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

    PhysicalPlan::PhysicalTableCreate* LogicalTableCreate::ToPhysical(QueryContext& context){
        Headers::Index index(this->primaryKey.Data(), this->primaryKey.Size());
        return context._compileContext.Allocate<PhysicalPlan::PhysicalTableCreate>(
            this->sessionId,
            this->table,
            this->columns,
            index,
            this->constraintName
        );
    }

    LogicalIndexCreate::LogicalIndexCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        DataTypes::String& constraintName,
        DataStructures::PolymorphicArray<column_index_t> &columns
    ) : LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

    PhysicalPlan::PlanNode* LogicalIndexCreate::ToPhysical(QueryContext& context){
        return context._compileContext.Allocate<PhysicalPlan::PhysicalIndexCreate>(this->sessionId, this->table, this->constraintName, this->columns);
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::NewColumn *column
    ) : LogicalPlan(sessionId), table(table), type(type) {
        this->column = { .addColumn = column };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::AlterColumn *column
    ) : LogicalPlan(sessionId), table(table), type(type) {
        this->column = { .alterColumn = column };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::RenameColumn *column
    ) : LogicalPlan(sessionId), table(table), type(type) {
        this->column = { .renameColumn = column };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::DropColumn *column
    ) : LogicalPlan(sessionId), table(table), type(type) {
        this->column = { .dropColumn = column };
    }

    PhysicalPlan::PlanNode* LogicalAlterTable::ToPhysical(QueryContext& context){
        switch (this->type) {
            case Constants::AlterTableType::AddColumn:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalAddColumn>(this->sessionId, this->table, this->column.addColumn);
            case Constants::AlterTableType::AlterColumn:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalAlterColumn>(this->sessionId, this->table, this->column.alterColumn);
            case Constants::AlterTableType::RenameColumn:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalRenameColumn>(this->sessionId, this->table, this->column.renameColumn);
            case Constants::AlterTableType::DropColumn:
                return context._compileContext.Allocate<PhysicalPlan::PhysicalDropColumn>(this->sessionId, this->table, this->column.dropColumn);
            default:
                return nullptr;
        }
    }
}
