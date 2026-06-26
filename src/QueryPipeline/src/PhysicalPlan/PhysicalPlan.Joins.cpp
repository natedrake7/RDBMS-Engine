#include <cmath>

#include "Database.h"
#include "../../include/PhysicalPlan.h"
#include "Contexts/ExecutionContext.h"

namespace QueryPipeline::PhysicalPlan {
    void PerformNullJoin(
        const ::Memory::IAllocator* allocator,
        ExecutionResult& result,
        CoreEngine::StorageTypes::RID* outerRow,
        const Int numberOfColumns
    ){
        // outerRow->Join(
        // Pages::RowView::NullReference(
        //         allocator,
        //         numberOfColumns
        //     )
        // );
        // result.rows.Push(outerRow);
    }

    void PerformJoin(
        const ::Memory::IAllocator* allocator,
        ExecutionResult& result,
        const CoreEngine::StorageTypes::RID* outerRow,
        const CoreEngine::StorageTypes::RID* innerRow
    ){
        // const auto outerCopy = Pages::RowView::Copy(
        //     outerRow,
        //     allocator
        // );
        // outerCopy->Join(innerRow);
        // result.rows.Push(outerCopy);
    }

    void VectorBatch::AllocateColumns(const Memory::IAllocator* allocator, const Int numberOfColumns){
        this->_columns = static_cast<CoreEngine::DataVector**>(
            allocator->AllocateRaw(sizeof(CoreEngine::DataVector*)*numberOfColumns)
        );
    }

    void VectorBatch::SetColumn(CoreEngine::DataVector* columnData, const Int columnIndex) const{
        this->_columns[columnIndex] = columnData;
    }

    ExecutionResult PhysicalNestedLoopInnerJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const {
        const auto* allocator = context.GetAllocator();

        auto result = ExecutionResult(context);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Join,
            context
        );

        bool canFetchMore = true;

        while (canFetchMore) {
            auto rightResult = this->right->Execute(context);
            canFetchMore = rightResult.canFetchMore;

            // for (const auto& outerRow: leftResult.rows) {
            //     for (const auto& innerRow: rightResult.rows) {
            //         evaluationContext.row = &outerRow;
            //         evaluationContext.joinRow = &innerRow;
            //         if (!Expressions::EvaluateExpression(this->expression, evaluationContext).AsBool())
            //             continue;
            //
            //          // PerformJoin(allocator, result, outerRow, innerRow);
            //     }
            // }
        }

        return result;
    }

    PhysicalNestedLoopInnerJoin::PhysicalNestedLoopInnerJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* joinCondition
    ) : left(left), right(right), expression(joinCondition) {}

    ExecutionResult PhysicalNestedLoopInnerJoin::Execute(CoreEngine::ExecutionContext& context) {
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);
        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    ExecutionResult PhysicalNestedLoopLeftJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const{
        const auto* allocator = context.GetAllocator();

        auto result = ExecutionResult(context);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Join,
            context
        );

        bool canFetchMore = true;

        // DataStructures::PolymorphicArray<bool> matchedRows(
        //     allocator,
        //     leftResult.rows.Size()
        // );
        // matchedRows.AlignSize();
        //
        // auto rightNumberOfColumns = 0;
        // while (canFetchMore) {
        //     auto rightResult = this->right->Execute(context);
        //     rightNumberOfColumns = rightResult.columns.Size();
        //     canFetchMore = rightResult.canFetchMore;
        //
        //     for (int i = 0;i < leftResult.rows.Size();i++){
        //         const auto& outerRow = leftResult.rows[i];
        //         for (const auto& innerRow: rightResult.rows) {
        //             evaluationContext.row = &outerRow;
        //             evaluationContext.joinRow = &innerRow;
        //             if (!Expressions::EvaluateExpression(this->expression, evaluationContext).AsBool())
        //                 continue;
        //
        //             matchedRows[i] = true;
        //             // PerformJoin(allocator, result, outerRow, innerRow);
        //         }
        //     }
        // }
        //
        // for (int i = 0;i < matchedRows.Size();i++){
        //     if (matchedRows[i])
        //         continue;
        //
        //     auto& outerRow = leftResult.rows[i];
        //     // outerRow->Join(
        //     //     Pages::RowView::NullReference(allocator, rightNumberOfColumns)
        //     // );
        //     // result.rows.Push(std::move(outerRow));
        // }

        return result;
    }

    PhysicalNestedLoopLeftJoin::PhysicalNestedLoopLeftJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* expression
    ) : left(left), right(right), expression(expression) {}

    ExecutionResult PhysicalNestedLoopLeftJoin::Execute(CoreEngine::ExecutionContext& context) {
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);
        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    PhysicalNestedLoopFullJoin::PhysicalNestedLoopFullJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* joinCondition
    ) : left(left), right(right), joinCondition(joinCondition) {}

    ExecutionResult PhysicalNestedLoopFullJoin::Execute(CoreEngine::ExecutionContext& context) {
        // auto* result = new ExecutionResult();
        //
        // auto* leftResult = this->left->Execute(properties);
        // auto* rightResult = this->right->Execute(properties);
        //
        // DataStructures::PolymorphicArray leftMatched(leftResult->rows.size(), false);
        // DataStructures::PolymorphicArray rightMatched(rightResult->rows.size(), false);
        //
        // Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);
        //
        // for (int i = 0;i < leftResult->rows.size();i++) {
        //   for (int j = 0;j < rightResult->rows.size();j++) {
        //     auto& outerRow = leftResult->rows[i];
        //     auto& innerRow = rightResult->rows[j];
        //
        //     context.outerRow = &outerRow;
        //     context.innerRow = &innerRow;
        //
        //     if (!this->joinCondition->Evaluate(context).AsBool())
        //       continue;
        //
        //     outerRow.Join(&innerRow);
        //     result->rows.push_back(outerRow);
        //     leftMatched[i] = true;
        //     rightMatched[j] = true;
        //   }
        // }
        //
        // for (int i = 0;i < leftResult->rows.size();i++) {
        //   if (leftMatched[i])
        //     continue;
        //
        //   auto& outerRow = leftResult->rows[i];
        //   outerRow.LeftJoin(rightResult->columns);
        //   result->rows.push_back(outerRow);
        // }
        //
        // for (int i = 0;i < rightResult->rows.size(); i++) {
        //   if (rightMatched[i])
        //     continue;
        //
        //   auto& innerRow = rightResult->rows[i];
        //   innerRow.RightJoin(rightResult->columns);
        //   result->rows.push_back(innerRow);
        // }
        //
        //
        // delete leftResult;
        // delete rightResult;
        //
        // return result;
    }

    ExecutionResult PhysicalMergeInnerJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const {
        using CompOperator = Comparators::Comparator;

        auto result = ExecutionResult(context);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Join,
            context
        );

        auto rightResult = this->right->Execute(context);

        // Int leftIndex = 0;
        // Int rightIndex = 0;
        //
        // const auto leftRowsCount = leftResult->rows.Size();
        // const auto rightRowsCount = rightResult->rows.Size();
        //
        // const auto outerRowSize = leftResult->columns.Size();
        //
        // while (leftIndex < leftRowsCount){
        //   auto& outerRow = leftResult->rows[leftIndex];
        //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
        //
        //   while (rightIndex < rightRowsCount){
        //     const auto& innerRow = rightResult->rows[rightIndex];
        //
        //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
        //
        //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
        //
        //     if (comparison == CompOperator::Less)
        //       break;
        //
        //     if (comparison == CompOperator::Greater){
        //       rightIndex++;
        //       continue;
        //     }
        //
        //     outerRow.Join(&innerRow);
        //     result->rows.push_back(outerRow);
        //
        //     rightIndex++;
        //   }
        //
        //   if (rightIndex >= rightRowsCount
        //       && leftIndex < leftRowsCount
        //       && rightResult->canFetchMore
        //   ){
        //     delete rightResult;
        //     rightResult = this->right->Execute(properties);
        //     rightIndex = 0;
        //   }
        //
        //   leftIndex++;
        // }
        //
        // if (rightIndex < rightRowsCount){
        //   const auto& lastUsedRow = rightResult->rows[rightIndex];
        //   this->right->UpdateScanState(lastUsedRow.GetId());
        // }
        //
        // delete rightResult;
        // result->canFetchMore = leftResult->canFetchMore;

        return result;
    }

    PhysicalMergeInnerJoin::PhysicalMergeInnerJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* expression,
        DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
        DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
    ) : left(left), right(right), expression(expression),
        leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)) {}

    ExecutionResult PhysicalMergeInnerJoin::Execute(CoreEngine::ExecutionContext& context) {
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);

        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    ExecutionResult PhysicalMergeLeftJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const {
        // using CompOperator = DataTypes::Indexing::Key::ComparisonResult;
        //
        // auto* result = new ExecutionResult();
        //
        // Expressions::EvaluationContext context(
        //   Expressions::EvaluationContext::EvaluationContextType::Join,
        //   properties.variables
        // );
        //
        // auto* rightResult = this->right->Execute(properties);
        //
        // Int leftIndex = 0;
        // Int rightIndex = 0;
        //
        // const auto leftRowsCount = leftResult->rows.size();
        // const auto rightRowsCount = rightResult->rows.size();
        //
        // const auto outerRowSize = static_cast<Int>(leftResult->columns.size());
        //
        // while (leftIndex < leftRowsCount){
        //   auto& outerRow = leftResult->rows[leftIndex];
        //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
        //
        //   bool hasMatch = false;
        //
        //   while (rightIndex < rightRowsCount){
        //     auto& innerRow = rightResult->rows[rightIndex];
        //
        //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
        //
        //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
        //
        //     if (comparison == CompOperator::Less)
        //       break;
        //
        //     if (comparison == CompOperator::Greater){
        //       rightIndex++;
        //       continue;
        //     }
        //
        //     hasMatch = true;
        //
        //     outerRow.Join(&innerRow);
        //     result.rows.push_back(outerRow);
        //
        //     rightIndex++;
        //   }
        //
        //   if (rightIndex >= rightRowsCount
        //       && leftIndex < leftRowsCount
        //       && rightResult->canFetchMore
        //   ){
        //     delete rightResult;
        //     rightResult = this->right->Execute(properties);
        //     rightIndex = 0;
        //   }
        //
        //   if (!hasMatch){
        //     outerRow.LeftJoin(rightResult->columns);
        //     result->rows.push_back(outerRow);
        //   }
        //
        //   leftIndex++;
        // }
        //
        // if (rightIndex < rightRowsCount){
        //   const auto& lastUsedRow = rightResult->rows[rightIndex];
        //   this->right->UpdateScanState(lastUsedRow.GetId());
        // }
        //
        // delete rightResult;
        // result->canFetchMore = leftResult->canFetchMore;

        // return result;
    }

    PhysicalMergeLeftJoin::PhysicalMergeLeftJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* expression,
        DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
        DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
    ) : left(left), right(right), expression(expression),
        leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)) {}

    ExecutionResult PhysicalMergeLeftJoin::Execute(CoreEngine::ExecutionContext& context) {
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);

        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    ExecutionResult PhysicalMergeFullJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const {
        // using CompOperator = DataTypes::Indexing::Key::ComparisonResult;
        //
        // auto* result = new ExecutionResult();
        //
        // Expressions::EvaluationContext context(
        //   Expressions::EvaluationContext::EvaluationContextType::Join,
        //   properties.variables
        // );
        //
        // auto* rightResult = this->right->Execute(properties);
        //
        // Int leftIndex = 0;
        // Int rightIndex = 0;
        //
        // const auto leftRowsCount = leftResult->rows.size();
        // const auto rightRowsCount = rightResult->rows.size();
        //
        // const auto outerRowSize = static_cast<Int>(leftResult->columns.size());
        //
        // while (leftIndex < leftRowsCount){
        //   auto& outerRow = leftResult->rows[leftIndex];
        //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
        //
        //   bool hasMatch = false;
        //
        //   while (rightIndex < rightRowsCount){
        //     auto& innerRow = rightResult->rows[rightIndex];
        //
        //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
        //
        //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
        //
        //     if (comparison == CompOperator::Less)
        //       break;
        //
        //     if (comparison == CompOperator::Greater){
        //       innerRow.RightJoin(leftResult->columns);
        //       result->rows.push_back(innerRow);
        //       rightIndex++;
        //       continue;
        //     }
        //
        //     hasMatch = true;
        //
        //     outerRow.Join(&innerRow);
        //     result->rows.push_back(outerRow);
        //     rightIndex++;
        //   }
        //
        //   if (rightIndex >= rightRowsCount
        //       && leftIndex < leftRowsCount
        //       && rightResult->canFetchMore
        //   ){
        //     delete rightResult;
        //     rightResult = this->right->Execute(properties);
        //     rightIndex = 0;
        //   }
        //
        //   if (!hasMatch){
        //     outerRow.LeftJoin(rightResult->columns);
        //     result->rows.push_back(outerRow);
        //   }
        //
        //   leftIndex++;
        // }
        //
        // if (rightIndex < rightRowsCount){
        //   const auto& lastUsedRow = rightResult->rows[rightIndex];
        //   this->right->UpdateScanState(lastUsedRow.GetId());
        // }
        //
        // delete rightResult;
        // result->canFetchMore = leftResult->canFetchMore;

        // return result;
    }

    PhysicalMergeFullJoin::PhysicalMergeFullJoin(
        PlanNode* left,
        PlanNode* right,
        Expressions::Expression* expression,
        DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
        DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
    ) : left(left), right(right), expression(expression),
        leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)) {}

    ExecutionResult PhysicalMergeFullJoin::Execute(CoreEngine::ExecutionContext& context) {
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);

        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    ExecutionResult PhysicalCrossInnerJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const{
        const auto* allocator = context.GetAllocator();
        auto result = ExecutionResult(context);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Join,
            context
        );

        bool canFetchMore = true;
        while (canFetchMore) {
            auto rightResult = this->right->Execute(context);
            canFetchMore = rightResult.canFetchMore;

            // for (int i = 0;i < leftResult.rows.Size();i++){
            //     const auto& outerRow = leftResult.rows[i];
            //     for (const auto& innerRow: rightResult.rows) {
            //         // evaluationContext.row = outerRow;
            //         // evaluationContext.joinRow = innerRow;
            //         // PerformJoin(allocator, result, outerRow, innerRow);
            //     }
            // }
        }

        return result;
    }

    PhysicalCrossInnerJoin::PhysicalCrossInnerJoin(PlanNode* left, PlanNode* right)
        : left(left), right(right){}

    ExecutionResult PhysicalCrossInnerJoin::Execute(CoreEngine::ExecutionContext& context){
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);
        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    ExecutionResult PhysicalCrossLeftJoin::ExecuteBatchJoin(
        CoreEngine::ExecutionContext& context,
        ExecutionResult& leftResult
    ) const{
        const auto* allocator = context.GetAllocator();
        auto result = ExecutionResult(context);

        auto rightResult = this->right->Execute(context);
        auto rightNumberOfColumns = rightResult.columns.Size();

        // if (rightResult.rows.Empty()){
        //     for (auto& outerRow : leftResult.rows){
        //         // PerformNullJoin(
        //         //     allocator,
        //         //     result,
        //         //     outerRow,
        //         //     rightNumberOfColumns
        //         // );
        //     }
        //
        //     return result;
        // }

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Join,
            context
        );

        bool canFetchMore = true;
        while (canFetchMore) {
            rightResult = this->right->Execute(context);
            canFetchMore = rightResult.canFetchMore;
            //
            // for (int i = 0;i < leftResult.rows.Size();i++){
            //     const auto& outerRow = leftResult.rows[i];
            //     for (const auto& innerRow: rightResult.rows) {
            //         // evaluationContext.row = outerRow;
            //         // evaluationContext.joinRow = innerRow;
            //         // PerformJoin(allocator, result, outerRow, innerRow);
            //     }
            // }
        }

        return result;
    }

    PhysicalCrossLeftJoin::PhysicalCrossLeftJoin(PlanNode* left, PlanNode* right)
        : left(left), right(right) {}

    ExecutionResult PhysicalCrossLeftJoin::Execute(CoreEngine::ExecutionContext& context){
        auto leftResult = this->left->Execute(context);
        auto result = this->ExecuteBatchJoin(context, leftResult);
        result.canFetchMore = leftResult.canFetchMore;
        return result;
    }

    PhysicalCrossFullJoin::PhysicalCrossFullJoin(PlanNode* left, PlanNode* right)
        : left(left), right(right) {}

    ExecutionResult PhysicalCrossFullJoin::Execute(CoreEngine::ExecutionContext& context){
        return ExecutionResult(context);
    }
}
