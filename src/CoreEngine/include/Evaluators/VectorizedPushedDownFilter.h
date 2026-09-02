#pragma once
#include "Expression.h"
#include "../Contexts/ExecutionContext.h"
#include "../Vectorization/Vectorization.h"
#include "../DataStorage/FilterColumnInfo.h"


namespace CoreEngine{
    namespace StorageTypes{
        struct FilterColumnInfo;
    }

    class VectorizedPushedDownFilter{
        DataStructures::PolymorphicArray<StorageTypes::RID> _candidates;

        const Expressions::Expression* _expression;
        const DataStructures::PolymorphicArray<StorageTypes::FilterColumnInfo>* _filterColumns;
        DataStructures::StaticArray<Storage::FileKey, StorageTypes::RID::Count> _fileKeys;
        const ExecutionContext* _executionContext;

        Int _schemaWidth;

        public:
        VectorizedPushedDownFilter(
            const Expressions::Expression* expression,
            const DataStructures::PolymorphicArray<StorageTypes::FilterColumnInfo>* filterColumns,
            const ExecutionContext* executionContext,
            const UnsignedSmallInt slotIndex,
            const Int schemaWidth
        ):  _candidates(executionContext->GetAllocator()), _expression(expression),
            _filterColumns(filterColumns), _fileKeys(executionContext->GetFileKeys(slotIndex)),
            _executionContext(executionContext), _schemaWidth(schemaWidth){}

        inline void AddCandidate(const StorageTypes::RID& candidate){
            this->_candidates.Push(candidate);
        }

        inline void Start(const Int candidatesSize){
            this->_candidates.Clear();
            this->_candidates.Reserve(candidatesSize);
        }

        inline void Filter(DataStructures::PolymorphicArray<StorageTypes::RID>* result){
            if (this->_candidates.Empty())
                return;

            const auto* allocator = this->_executionContext->GetAllocator();
            const auto* fileKeysData = this->_fileKeys.Data();

            auto allocationStart = allocator->RecordAllocationStart();

            DataChunk chunk;
            chunk.AllocateColumns(allocator, this->_candidates.Size(), this->_schemaWidth);

            for (auto i = 0; i < this->_filterColumns->Size(); i++) {
                const auto& columnInfo = (*this->_filterColumns)[i];

                auto* vector = DataVector::FlatVector(allocator, columnInfo._type, this->_candidates.Size());
                columnInfo._function(fileKeysData, allocator, this->_candidates, vector, columnInfo._ordinalPosition);
                chunk.SetColumn(vector, columnInfo._schemaPosition);
            }

            const auto* mask = Expressions::EvaluateExpression(this->_expression, this->_executionContext, &chunk);

            const auto* __restrict__ maskValidity = mask->_validity;
            const auto* __restrict__ selected = mask->DataAs<bool>();
            const auto* __restrict__ candidatesData = this->_candidates.Data();
            auto* __restrict__ out = result->Data() + result->Size();

            Int selectedCount = 0;
            UnsignedBigInt anyNull = 0;
            const auto wordsCount = mask->WordsCount();
            for (auto w = 0; w < wordsCount; w++)
                anyNull |= maskValidity[w];

            const auto candidatesSize = this->_candidates.Size();
            if (anyNull == 0){
                for (auto i = 0;i < candidatesSize; i++){
                    out[selectedCount] = candidatesData[i];
                    selectedCount += selected[i];
                }
            }
            else{
                for (auto i = 0;i < candidatesSize; i++){
                    out[selectedCount] = candidatesData[i];
                    selectedCount += selected[i] & !mask->GetNullValue(i);
                }
            }

            allocator->ReleaseFromAllocationStep(allocationStart);
            result->SetNewSize(result->Size() + selectedCount);
        }
    };
}
