#include "../../../../include/Evaluators/Kernels/Vectorized/Vectorized.JumpTables.h"
#include "../../../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "DataStorage/Table.h"
#include "Evaluators/Kernels/Vectorized/Vectorized.Binary.h"

namespace CoreEngine::VectorizedKernels{
    namespace{
        using KernelTable = DataStructures::StaticArray<
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT>,
            DATATYPE_COUNT
        >;

        template<typename T>
        constexpr void RegisterComparisons(KernelTable& table){
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
        constexpr void RegisterArithmetic(KernelTable& table){
            constexpr auto TYPE = static_cast<size_t>(DataTypes::DataTypeOf<T>());
            using Op = Expressions::BinaryOperator;

            table[static_cast<size_t>(Op::Add)][TYPE] = &BinaryArithmeticKernel<T, std::plus<T>>;
            table[static_cast<size_t>(Op::Subtract)][TYPE] = &BinaryArithmeticKernel<T, std::minus<T>>;
            table[static_cast<size_t>(Op::Multiply)][TYPE] = &BinaryArithmeticKernel<T, std::multiplies<T>>;
            table[static_cast<size_t>(Op::Divide)][TYPE] = &BinaryDivideKernel<T>;
            table[static_cast<size_t>(Op::Modulo)][TYPE] = &BinaryModuloKernel<T>;
        }

        constexpr void RegisterStringOperations(KernelTable& table){
            constexpr auto TYPE = static_cast<size_t>(DataTypes::DataTypeOf<DataTypes::StringValue>());
            using Op = Expressions::BinaryOperator;
            table[static_cast<size_t>(Op::Add)][TYPE] = &BinaryArithmeticKernel<DataTypes::StringValue, std::plus<DataTypes::StringValue>>;
        }

        template<typename... Ts> constexpr void RegisterComparisonsFor(KernelTable& table){ (RegisterComparisons<Ts>(table), ...); }
        template<typename... Ts> constexpr void RegisterArithmeticFor(KernelTable& table){ (RegisterArithmetic<Ts>(table), ...); }

        template<typename... Ts>
        constexpr auto MakeBinaryKernelTable(){
            KernelTable table = {};

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

        constexpr auto MakeLogicalKernelTable(){
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, Expressions::LOGICAL_TYPE_COUNT> table{};
            table[static_cast<size_t>(Expressions::LogicalType::And)] = &LogicalAndKernel;
            table[static_cast<size_t>(Expressions::LogicalType::Or)] = &LogicalOrKernel;
            table[static_cast<size_t>(Expressions::LogicalType::Not)] = &LogicalNotKernel;
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

        inline constexpr auto BINARY_KERNELS = MakeBinaryKernelTable<
            bool, TinyInt, SmallInt, Int, BigInt,
            DataTypes::Decimal, DataTypes::StringValue, DataTypes::DateTime,
            DataTypes::Guid, DataTypes::JsonBinary
        >();

        inline constexpr auto LOGICAL_KERNELS = MakeLogicalKernelTable();
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
}
