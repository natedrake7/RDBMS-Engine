#include "../../../../include/Evaluators/Kernels/Vectorized/Vectorized.JumpTables.h"
#include "../../../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "DataStorage/Table.h"
#include "Evaluators/Kernels/Vectorized/Vectorized.Binary.h"

namespace CoreEngine::VectorizedKernels{
    namespace{
        using BinaryKernelTable = DataStructures::StaticArray<
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT>,
            DATATYPE_COUNT
        >;

        using CastKernelTable = DataStructures::StaticArray<
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT>,
            DATATYPE_COUNT
        >;

        template<typename...> struct TypeList{};

        using CastTypes = TypeList<
            bool, TinyInt, SmallInt, Int, BigInt,
            DataTypes::Decimal, DataTypes::StringValue,
            DataTypes::DateTime, DataTypes::Guid, DataTypes::JsonBinary
        >;

        template<typename TFrom, typename TTo>
        constexpr void RegisterCastPair(CastKernelTable& table){
            constexpr auto from = DataTypes::DataTypeOf<TFrom>();
            constexpr auto to   = DataTypes::DataTypeOf<TTo>();

            if constexpr (
                !std::is_same_v<TFrom, TTo> &&
                DataTypes::Coercions::IsCoercionAllowed(from, to, true)
            ){
                table[static_cast<size_t>(from)][static_cast<size_t>(to)] = &CoreEngine::VectorizedKernels::CastKernel<TFrom, TTo>;
            }
        }

        template<typename TFrom, typename... TTos>
        constexpr void RegisterCastRow(CastKernelTable& table, TypeList<TTos...>) {
            (RegisterCastPair<TFrom, TTos>(table), ...);
        }

        template<typename ...TFroms>
        constexpr void RegisterCastMatrix(CastKernelTable& table, TypeList<TFroms...> all) {
            (RegisterCastRow<TFroms>(table, all), ...);
        }

        template<typename T>
        constexpr void RegisterComparisons(BinaryKernelTable& table){
            constexpr auto TYPE = static_cast<size_t>(DataTypes::DataTypeOf<T>());   // derive, don't pass
            using Op = Expressions::BinaryOperator;

            table[static_cast<size_t>(Op::Equal)][TYPE] = &BinaryComparisonKernel<T, std::equal_to<T>>;
            table[static_cast<size_t>(Op::NotEqual)][TYPE] = &BinaryComparisonKernel<T, std::not_equal_to<T>>;
            table[static_cast<size_t>(Op::Greater)][TYPE] = &BinaryComparisonKernel<T, std::greater<T>>;
            table[static_cast<size_t>(Op::GreaterEqual)][TYPE] = &BinaryComparisonKernel<T, std::greater_equal<T>>;
            table[static_cast<size_t>(Op::Less)][TYPE] = &BinaryComparisonKernel<T, std::less<T>>;
            table[static_cast<size_t>(Op::LessEqual)][TYPE] = &BinaryComparisonKernel<T, std::less_equal<T>>;
        }

        template<typename T>
        constexpr void RegisterArithmetic(BinaryKernelTable& table){
            constexpr auto TYPE = static_cast<size_t>(DataTypes::DataTypeOf<T>());
            using Op = Expressions::BinaryOperator;

            table[static_cast<size_t>(Op::Add)][TYPE] = &BinaryArithmeticKernel<T, std::plus<T>>;
            table[static_cast<size_t>(Op::Subtract)][TYPE] = &BinaryArithmeticKernel<T, std::minus<T>>;
            table[static_cast<size_t>(Op::Multiply)][TYPE] = &BinaryArithmeticKernel<T, std::multiplies<T>>;
            table[static_cast<size_t>(Op::Divide)][TYPE] = &BinaryDivideKernel<T>;
            table[static_cast<size_t>(Op::Modulo)][TYPE] = &BinaryModuloKernel<T>;
        }

        constexpr void RegisterStringOperations(BinaryKernelTable& table){
            constexpr auto TYPE = static_cast<size_t>(DataTypes::DataTypeOf<DataTypes::StringValue>());
            using Op = Expressions::BinaryOperator;
            table[static_cast<size_t>(Op::Add)][TYPE] = &BinaryArithmeticKernel<DataTypes::StringValue, std::plus<DataTypes::StringValue>>;
        }

        template<typename... Ts> constexpr void RegisterComparisonsFor(BinaryKernelTable& table){ (RegisterComparisons<Ts>(table), ...); }
        template<typename... Ts> constexpr void RegisterArithmeticFor(BinaryKernelTable& table){ (RegisterArithmetic<Ts>(table), ...); }

        constexpr auto MakeBinaryKernelTable(){
            BinaryKernelTable table = {};

            RegisterComparisonsFor<
                bool, TinyInt, SmallInt, Int, BigInt,
                DataTypes::Decimal, DataTypes::DateTime, DataTypes::Guid, DataTypes::StringValue
            >(table);
            RegisterArithmeticFor<TinyInt, SmallInt, Int, BigInt, DataTypes::Decimal>(table);
            RegisterStringOperations(table);   // Add(concat) + IgnoreCase — genuinely string-specific

            return table;
        }

        template <typename... Ts>
        constexpr auto MakeMaterializerTable() {
            DataStructures::StaticArray<StorageTypes::TableMaterializationFunction, DATATYPE_COUNT> table{};  // all nullptr
            ((table[static_cast<size_t>(DataTypes::DataTypeOf<Ts>())] =
                  &StorageTypes::Table::MaterializeColumn<Ts>
            ), ...);
            return table;
        }

        template <typename... Ts>
        constexpr auto MakeConstantKernelTable(){
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT> table{};  // all nullptr
            ((table[static_cast<size_t>(DataTypes::DataTypeOf<Ts>())] =
                  &ConstantScanKernel<Ts>
            ), ...);
            return table;
        }

        template<typename... Ts>
        constexpr auto MakeVariableKernelTable(){
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT> table{};  // all nullptr
            ((table[static_cast<size_t>(DataTypes::DataTypeOf<Ts>())] =
                  &VariableScanKernel<Ts>
            ), ...);
            return table;
        }

        constexpr auto MakeLogicalKernelTable(){
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, Expressions::LOGICAL_TYPE_COUNT> table{};
            table[static_cast<size_t>(Expressions::LogicalType::And)] = &LogicalAndKernel;
            table[static_cast<size_t>(Expressions::LogicalType::Or)] = &LogicalOrKernel;
            table[static_cast<size_t>(Expressions::LogicalType::Not)] = &LogicalNotKernel;
            return table;
        }

        constexpr auto MakeCastKernelTable(){
            CastKernelTable table = {};
            RegisterCastMatrix(table, CastTypes{});
            return table;
        }

        // Types here are the *in-vector* representations, not the storage ones:
        // a String column materializes into StringValue entries.
        inline constexpr auto COLUMN_MATERIALIZERS = MakeMaterializerTable<
            bool, TinyInt, SmallInt, Int, BigInt,
            DataTypes::Decimal, DataTypes::StringValue, DataTypes::DateTime,
            DataTypes::Guid, DataTypes::JsonBinary
        >();

        inline constexpr auto CONSTANT_KERNELS = MakeConstantKernelTable<
            bool, TinyInt, SmallInt, Int, BigInt,
            DataTypes::Decimal, DataTypes::StringValue, DataTypes::DateTime,
            DataTypes::Guid, DataTypes::JsonBinary
        >();

        inline constexpr auto VARIABLE_KERNELS = MakeVariableKernelTable<
            bool, TinyInt, SmallInt, Int, BigInt,
            DataTypes::Decimal, DataTypes::StringValue, DataTypes::DateTime,
            DataTypes::Guid, DataTypes::JsonBinary
        >();

        inline constexpr auto BINARY_KERNELS = MakeBinaryKernelTable();

        inline constexpr auto LOGICAL_KERNELS = MakeLogicalKernelTable();

        inline constexpr auto CAST_KERNELS = MakeCastKernelTable();
    }

    StorageTypes::TableMaterializationFunction JumpTables::GetMaterializationFunction(DataType type){
        return COLUMN_MATERIALIZERS[static_cast<Int>(type)];
    }

    Expressions::VectorizedKernelFunction JumpTables::GetConstantKernel(DataType type){
        return CONSTANT_KERNELS[static_cast<Int>(type)];
    }

    Expressions::VectorizedKernelFunction JumpTables::GetBinaryKernel(
        Expressions::BinaryOperator _operator,
        DataType type
    ){
        return BINARY_KERNELS[static_cast<Int>(_operator)][static_cast<Int>(type)];
    }

    Expressions::VectorizedKernelFunction JumpTables::GetLogicalKernel(Expressions::LogicalType type){
        return LOGICAL_KERNELS[static_cast<Int>(type)];
    }

    Expressions::VectorizedKernelFunction JumpTables::GetCastKernel(DataType fromType, DataType toType){
        return CAST_KERNELS[static_cast<Int>(fromType)][static_cast<Int>(toType)];
    }

    Expressions::VectorizedKernelFunction JumpTables::GetVariableKernel(DataType type){
        return VARIABLE_KERNELS[static_cast<Int>(type)];
    }
}
