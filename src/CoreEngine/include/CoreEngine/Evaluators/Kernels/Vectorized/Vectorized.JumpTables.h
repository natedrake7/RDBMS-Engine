#pragma once
#include <CoreEngine/Evaluators/Kernels/Vectorized/Vectorized.Binary.h>
#include <CoreEngine/Evaluators/Kernels/Vectorized/VectorizedKernels.h>
#include <CoreEngine/DataStorage/ColumnMaterializationInfo.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/DataStorage/FilterColumnInfo.h>
#include <CoreEngine/DataStorage/Table.h>

namespace CoreEngine::VectorizedKernels{

    namespace Detail{
        using CastKernelTable = DataStructures::StaticArray<
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT>,
            DATATYPE_COUNT
        >;

        template <typename Category>
        consteval bool Shares(const std::meta::info op, const std::meta::info type){
            return Reflection::HasAnnotation<Category>(op) && Reflection::HasAnnotation<Category>(type);
        }

        template <typename Fn, typename Make>
        consteval auto MakeTypeTable(Make make){
            DataStructures::StaticArray<Fn, DATATYPE_COUNT> table{};
            template for (constexpr auto e : Reflection::Enumerators<DataType>){
                using T = DataTypes::StorageOf<([:e:])>;
                if constexpr (!std::is_void_v<T>)
                    table[static_cast<std::size_t>([:e:])] = make.template operator()<T>();
            }
            return table;
        }

        consteval auto MakeCastKernelTable(){
            CastKernelTable table{};
            template for (constexpr auto from : Reflection::Enumerators<DataType>){
                template for (constexpr auto to : Reflection::Enumerators<DataType>){
                    using TFrom = DataTypes::StorageOf<([:from:])>;
                    using TTo   = DataTypes::StorageOf<([:to:])>;
                    if constexpr (
                        !std::is_void_v<TFrom> && !std::is_void_v<TTo> && !std::is_same_v<TFrom, TTo>
                                  && DataTypes::Coercions::IsCoercionAllowed([:from:], [:to:], true)
                        ) table[static_cast<std::size_t>([:from:])][static_cast<std::size_t>([:to:])] = &CastKernel<TFrom, TTo>;
                }
            }
            return table;
        }

        using BinaryKernelTable = DataStructures::StaticArray<
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, DATATYPE_COUNT>,
            Expressions::BINARY_OPERATORS_COUNT
        >;

        template <Expressions::BinaryOperator OP>
        using BinaryOperationFunctorOf = Expressions::BinaryOperatorFunctors::At<static_cast<std::size_t>(OP)>;

        consteval auto MakeBinaryKernelTable(){
            using namespace DataTypes::Traits;
            BinaryKernelTable table{};

            template for (constexpr auto op : Reflection::Enumerators<Expressions::BinaryOperator>){
                using operationFunctor = BinaryOperationFunctorOf<([:op:])>;
                constexpr auto leftTypeIndex = static_cast<std::size_t>([:op:]);

                template for (constexpr auto type : Reflection::Enumerators<DataType>){
                    using T = DataTypes::StorageOf<([:type:])>;
                    constexpr auto rightTypeIndex = static_cast<std::size_t>([:type:]);

                    if constexpr (!std::is_void_v<T>){
                        if constexpr (Shares<Comparison>(op, type) || Shares<CaseInsensitiveComparison>(op, type))
                            table[leftTypeIndex][rightTypeIndex] = &BinaryComparisonKernel<T, operationFunctor>;
                        else if constexpr (Shares<Arithmetic>(op, type) || Shares<Concatenation>(op, type)){
                            if constexpr (std::is_same_v<operationFunctor, Expressions::DivideTag>)
                                table[leftTypeIndex][rightTypeIndex] = &BinaryDivideKernel<T>;
                            else if constexpr (std::is_same_v<operationFunctor, Expressions::ModuloTag>)
                                table[leftTypeIndex][rightTypeIndex] = &BinaryModuloKernel<T>;
                            else
                                table[leftTypeIndex][rightTypeIndex] = &BinaryArithmeticKernel<T, operationFunctor>;
                        }
                    }
                }
            }
            return table;
        }

        consteval auto MakeLogicalKernelTable(){
            using Expressions::LogicalType;
            DataStructures::StaticArray<Expressions::VectorizedKernelFunction, Reflection::EnumCount<LogicalType>> table{};

            template for (constexpr auto e : Reflection::Enumerators<LogicalType>){
                constexpr auto type = [:e:];
                constexpr auto index = static_cast<std::size_t>(type);

                if constexpr (type == LogicalType::And)
                    table[index] = &LogicalAndKernel;
                else if constexpr (type == LogicalType::Or)
                    table[index] = &LogicalOrKernel;
                else if constexpr (type == LogicalType::Not)
                    table[index] = &LogicalNotKernel;
                else static_assert(
                    DataTypes::AlwaysFalse<decltype(type)>,
                    "LogicalType without a vectorized kernel"
                );
            }
            return table;
        }

        inline constexpr auto COLUMN_MATERIALIZERS = MakeTypeTable<StorageTypes::TableMaterializationFunction>(
            []<typename T>(){ return &StorageTypes::Table::MaterializeColumn<T>; }
        );
        inline constexpr auto PAGE_MATERIALIZERS = MakeTypeTable<StorageTypes::PageMaterializationFunction>(
            []<typename T>(){ return &StorageTypes::Table::MaterializeColumnFromPage<T>; }
        );

        inline constexpr auto CONSTANT_KERNELS = MakeTypeTable<Expressions::VectorizedKernelFunction>(
            []<typename T>(){ return &ConstantScanKernel<T>; }
        );
        inline constexpr auto VARIABLE_KERNELS = MakeTypeTable<Expressions::VectorizedKernelFunction>(
            []<typename T>(){ return &VariableScanKernel<T>; }
        );

        inline constexpr auto CAST_KERNELS = MakeCastKernelTable();
        inline constexpr auto BINARY_KERNELS = MakeBinaryKernelTable();
        inline constexpr auto LOGICAL_KERNELS = MakeLogicalKernelTable();
    }

    class JumpTables{


    public:
        static inline constexpr StorageTypes::PageMaterializationFunction GetPageMaterializationFunction(const DataType type){
            return Detail::PAGE_MATERIALIZERS[static_cast<std::size_t>(type)];
        }

        static inline constexpr StorageTypes::TableMaterializationFunction GetMaterializationFunction(const DataType type){
            return Detail::COLUMN_MATERIALIZERS[static_cast<std::size_t>(type)];
        }

        static inline constexpr Expressions::VectorizedKernelFunction GetConstantKernel(const DataType type){
            return Detail::CONSTANT_KERNELS[static_cast<std::size_t>(type)];
        }

        static inline constexpr Expressions::VectorizedKernelFunction GetBinaryKernel(
            const Expressions::BinaryOperator _operator,
            const DataType type
        ){
            return Detail::BINARY_KERNELS[static_cast<std::size_t>(_operator)][static_cast<std::size_t>(type)];
        }

        static inline constexpr Expressions::VectorizedKernelFunction GetLogicalKernel(const Expressions::LogicalType type){
            return Detail::LOGICAL_KERNELS[static_cast<std::size_t>(type)];
        }

        static inline constexpr Expressions::VectorizedKernelFunction GetCastKernel(const DataType fromType, const DataType toType){
            return Detail::CAST_KERNELS[static_cast<std::size_t>(fromType)][static_cast<std::size_t>(toType)];
        }

        static inline constexpr Expressions::VectorizedKernelFunction GetVariableKernel(const DataType type){
            return Detail::VARIABLE_KERNELS[static_cast<std::size_t>(type)];
        }
    };
}
