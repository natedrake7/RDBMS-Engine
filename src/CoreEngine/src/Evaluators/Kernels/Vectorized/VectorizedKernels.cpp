#include "../../../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "../../../../include/Evaluators/Expression.h"
#include "Contexts/ExecutionContext.h"

namespace CoreEngine::VectorizedKernels{
    DataVector* ColumnScanKernel(
        const Expressions::Expression* self,
        const ExecutionContext*,
        const DataChunk* chunk
    ){
        const auto* columnExpression = self->AsColumn();
        return chunk->_columns[columnExpression->_boundReference._position];
    }
}
