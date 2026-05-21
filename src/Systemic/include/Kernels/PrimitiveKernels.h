#pragma once
#include "../DataTypes/DataTypes.h"

namespace Kernels::Primitives{
    template <typename T>
    static void AddKernel(
        const T* __restrict lhs,
        const T* __restrict rhs,
        T* __restrict result,
        const Int size
    ){
        for (Int i = 0; i < size; ++i)
            result[i] = lhs[i] + rhs[i];
    }

    template <typename T>
    static void SubtractKernel(
        const T* __restrict lhs,
        const T* __restrict rhs,
        T* __restrict result,
        const Int size
    ){
        for (Int i = 0; i < size; ++i)
            result[i] = lhs[i] - rhs[i];
    }

    template <typename T>
    static void MultiplyKernel(
        const T* __restrict lhs,
        const T* __restrict rhs,
        T* __restrict result,
        const Int size
    ){
        for (Int i = 0; i < size; ++i)
            result[i] = lhs[i] * rhs[i];
    }

    template <typename T>
    static void DivideKernel(
        const T* __restrict lhs,
        const T* __restrict rhs,
        T* __restrict result,
        const Int size
    ){
            for (Int i = 0; i < size; ++i)
                result[i] = lhs[i] / rhs[i];
        }
}
