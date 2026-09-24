#pragma once
#include <concepts>

#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataTypes/DateTime.h"
#include "../../../../Systemic/include/DataTypes/Guid.h"
#include "../../Vectorization/Vectorization.h"

namespace DataTypes{
    struct LobReference;
}

namespace CoreEngine::StorageTypes{
    template<typename T>
    concept VariableSlot = requires(const T& value){
            { value.Data() } -> std::convertible_to<const object_t*>;
            { value.Size() } -> std::convertible_to<Int>;
    };

    template<typename T>
    concept LobCapableSlot = VariableSlot<T> && requires(const T& value){
        { value.IsLobReference() } -> std::same_as<bool>;
        { value.AsLobReference() } -> std::same_as<DataTypes::LobReference>;
    };

    template<typename Visitor>
    static decltype(auto) VisitVariableType(const DataType type, Visitor&& visitor){
        switch (type){
        case DataType::String:
            return visitor.template operator()<DataTypes::StringValue>();
        case DataType::Decimal:
            return visitor.template operator()<DataTypes::Decimal>();
        case DataType::Json:
            return visitor.template operator()<DataTypes::JsonBinary>();
        default:
            std::unreachable();
        }
    }

    template<typename Visitor>
    static decltype(auto) VisitFixedType(const DataType type, Visitor&& visitor){
        switch (type){
        case DataType::Bool:
            return visitor.template operator()<sizeof(bool)>();
        case DataType::TinyInt:
            return visitor.template operator()<sizeof(TinyInt)>();
        case DataType::SmallInt:
            return visitor.template operator()<sizeof(SmallInt)>();
        case DataType::Int:
            return visitor.template operator()<sizeof(Int)>();
        case DataType::BigInt:
            return visitor.template operator()<sizeof(BigInt)>();
        case DataType::DateTime:
            return visitor.template operator()<sizeof(DataTypes::DateTime)>();
        case DataType::Guid:
            return visitor.template operator()<sizeof(DataTypes::Guid)>();
        default:
            std::unreachable();
        }
    }

    template<typename Body>
    static void ForEachRow(const DataVector* __restrict__ vector, const UnsignedInt rowCount, Body&& body){
        switch (vector->_kind) {
        case DataVectorKind::Flat:{
            for (UnsignedInt i = 0; i < rowCount; i++)
                body(vector, i);
            return;
        }
        case DataVectorKind::Constant:{
            const auto* __restrict__ selection = vector->_selection;
            for (UnsignedInt i = 0; i < rowCount; i++)
                body(vector, *(selection + i));
            return;
        }
        case DataVectorKind::Dictionary:
            for (UnsignedInt i = 0; i < rowCount; i++)
                body(vector, 0u);
            return;
        }

        std::unreachable();
    }
}
