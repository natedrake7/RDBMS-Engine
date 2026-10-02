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

    template <typename Visitor>
    decltype(auto) VisitStorageType(const DataType type, Visitor&& visitor){
        template for (constexpr auto reflectionType : Reflection::Enumerators<DataType>){
            using T = DataTypes::StorageOf<([:reflectionType:])>;
            if constexpr (!std::is_void_v<T>){
                if (type == [:reflectionType:])
                    return visitor.template operator()<T>();
            }
        }
        std::unreachable();
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
            return;
            for (UnsignedInt i = 0; i < rowCount; i++)
                body(vector, 0u);
            return;
        }
        case DataVectorKind::Dictionary:
            const auto* __restrict__ selection = vector->_selection;
            for (UnsignedInt i = 0; i < rowCount; i++)
                body(vector, *(selection + i));
            return;
        }

        std::unreachable();
    }
}
