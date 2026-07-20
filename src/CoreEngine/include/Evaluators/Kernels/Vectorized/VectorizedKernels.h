#pragma once
#include <type_traits>

#include "../../Expression.h"
#include "../../../Contexts/ExecutionContext.h"
#include "../../../DataStorage/Table.h"
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
    // template<typename T>
    // concept PrimitiveColumn = std::is_trivially_copyable_v<T>
    //                        && !std::is_pointer_v<T>;

    // template <PrimitiveColumn T>
    // DataVector* PrimitiveColumnScanKernel(
    //     const Expressions::Expression* self,
    //     const ExecutionContext& context,
    //     const SelectionVector* sv
    // ){
    //     const auto* columnExpression = self->AsColumn();
    //     auto* table = context.GetTable(columnExpression->_slotIndex);
    //     auto* dataVector = context.Allocate<DataVector>(columnExpression->returnType);
    //
    //     dataVector->_data = static_cast<object_t*>(context.Allocate(sizeof(T) * sv->selectedRidsCount));
    //     if (sv->isIdentity){
    //         table->MaterializeColumn<T>(
    //             context, sv->selectedRidsCount,
    //             dataVector->_data,
    //             columnExpression->ordinalPosition
    //         );
    //     }
    //     else{
    //         table->MaterializeColumn<T>(
    //             context, sv, dataVector->_data,
    //             columnExpression->ordinalPosition
    //         );
    //     }
    //
    //     return dataVector;
    // }

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
