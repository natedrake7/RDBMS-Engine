#pragma once
#include "VectorizedKernels.h"
#include "../../../DataStorage/ColumnMaterializationInfo.h"
#include "../../../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine::VectorizedKernels{
    class JumpTables{
    public:
        static StorageTypes::TableMaterializationFunction GetMaterializationFunction(DataType type);
        static Expressions::VectorizedKernelFunction GetConstantKernel(DataType type);
        static Expressions::VectorizedKernelFunction GetBinaryKernel(Expressions::BinaryOperator _operator, DataType type);
    };
}
