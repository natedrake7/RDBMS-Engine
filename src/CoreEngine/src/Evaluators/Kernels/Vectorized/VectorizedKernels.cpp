#include "../../../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "../../../../include/Evaluators/Expression.h"


namespace CoreEngine::VectorizedKernels{
    DataVector* ColumnScanKernel(
        const Expressions::Expression* self,
        const DataChunk* chunk
    ){
        const auto* columnExpression = self->AsColumn();
        return chunk->_columns[columnExpression->_boundReference._position];
    }
}
