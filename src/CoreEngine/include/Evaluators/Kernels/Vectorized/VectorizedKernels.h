#pragma once
#include "../../../Vectorization/Vectorization.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine{
    struct SelectionVector;
    class ExecutionContext;
    struct DataVector;
}

namespace CoreEngine::VectorizedKernels{
    DataVector* ColumnScanKernel(
        const Expressions::Expression* self,
        const DataChunk* chunk
    );

    // template<typename T>
    // concept ImplementsSize = requires(T a) {
    //     a.Size();
    // };
    //
    // template<ImplementsSize T>
    // DataVector* ScalarColumnScanKernel(
    //     Expressions::Expression* self,
    //     const ExecutionContext& context,
    //     SelectionVector* sv
    // ){
    //     const auto* columnExpression = self->AsColumn();
    //     auto* table = context.GetTable(columnExpression->_slotIndex);
    //     auto* dataVector = context.Allocate<DataVector>(columnExpression->returnType);
    //
    //     dataVector->_data = static_cast<object_t*>(context.Allocate(sizeof(T) * sv->selectedRidsCount));
    //     // table->MaterializeColumnFromIndexPage(
    //     //     context, sv, dataVector->_data,
    //     //     sizeof(T), columnExpression->columnIndex
    //     // );
    //
    //     return dataVector;
    // }
}
