#include "../../../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "Evaluators/Kernels/Row/RowKernels.Cast.h"

namespace CoreEngine::RowKernels{
    template<typename...> struct TypeList {};
    using CastTypes = TypeList<
        bool, TinyInt,
        SmallInt, Int,
        BigInt,
        DataTypes::Decimal,
        DataTypes::StringValue,
        DataTypes::Guid,
        DataTypes::DateTime,
        DataTypes::JsonBinary
    >;

    template<typename TFrom, typename TTo>
    constexpr void RegisterCastPair(CastKernelTable& table) {
            constexpr auto from = DataTypes::DataTypeOf<TFrom>();
            constexpr auto to   = DataTypes::DataTypeOf<TTo>();

            if constexpr (
                !std::is_same_v<TFrom, TTo> &&
                DataTypes::Coercions::IsCoercionAllowed(from, to, true)
            ){
                table.cells[static_cast<Int>(from)][static_cast<Int>(to)]
                    = &CastRowKernel<TFrom, TTo>;
            }
        // else: pair can't be code-generated -> CastRowKernel<> is never named,
        //       so no instantiation, no static_assert, slot stays nullptr.
    }

    template<typename TFrom, typename... TTos>
    constexpr void RegisterCastRow(CastKernelTable& table, TypeList<TTos...>) {
        (RegisterCastPair<TFrom, TTos>(table), ...);
    }

    template<typename... TFroms>
    constexpr void RegisterCastMatrix(CastKernelTable& table, TypeList<TFroms...> all) {
        (RegisterCastRow<TFroms>(table, all), ...);
    }

    constexpr CastKernelTable RegisterCastKernels(){
        CastKernelTable table{};
        RegisterCastMatrix(table, CastTypes{});
        return table;
    }

    inline constexpr CastKernelTable CAST_KERNEL_TABLE = RegisterCastKernels();

    Expressions::RowKernelFunction LookupCastKernel(DataType fromType, DataType toType){
        return CAST_KERNEL_TABLE.cells[static_cast<Int>(fromType)][static_cast<Int>(toType)];
    }
}
