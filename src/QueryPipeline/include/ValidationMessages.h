#pragma once
#include "../../Systemic/include/Converter.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace QueryPipeline::Messages {
    static DataTypes::String ADDED_VARIABLE(
        const DataTypes::StringView& name,
        const ::Memory::IAllocator* allocator
    ){ return DataTypes::String::Concat(allocator, "Added variable: ", name); };
    static DataTypes::String FAILED_TO_GET_ROLE(
        const DataTypes::StringView& role,
        const ::Memory::IAllocator* allocator
    ){
        return DataTypes::String::Concat(allocator, "Failed to get role: ", role);
    }

    static constexpr DataTypes::StringView FAILED_TO_ADD_VARIABLE = "Failed to add variable";

    static constexpr DataTypes::StringView FAILED_TO_RETRIEVE_USER_SESSION = "Failed to retrieve user session";
    static constexpr DataTypes::StringView FAILED_TO_CREATE_USER = "Failed to create user";


    static constexpr DataTypes::StringView USE_DATABASE_SUCCESS = "Database changed successfully";
    static constexpr DataTypes::StringView USE_DATABASE_FAIL = "Failed to use Database";

    static DataTypes::String INSERT_ROWS_FROM_CHILD_QUERY(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter<Int>::Itos(count, allocator);
        return DataTypes::String::Concat(allocator, "Inserted ", count, " rows from child query");
    }
    static DataTypes::String INSERT_ROWS_FROM_FIELDS(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter<Int>::Itos(count, allocator);
        return DataTypes::String::Concat(allocator, "Inserted ", parsedInt, " rows from fields");
    }

    static constexpr DataTypes::StringView FAILED_TO_FETCH_USER_SESSION = "Failed to fetch user session";
    static constexpr DataTypes::StringView EMPTY_USERNAME = "username cannot be empty";
    static constexpr DataTypes::StringView EMPTY_PASSWORD = "password cannot be empty";
    static constexpr DataTypes::StringView EMPTY_ROLE = "role cannot be empty";
    static constexpr DataTypes::StringView FAILED_TO_FETCH_ROLE = "Failed to find given role";
    static constexpr DataTypes::StringView USER_ALREADY_EXISTS = "User with username already exists";
    static constexpr DataTypes::StringView USER_DOES_NOT_EXIST = "User does not exist";

    static DataTypes::String INVALID_DATATYPE_CONVERSION_MESSAGE(
        const ::Memory::IAllocator* allocator,
        const DataType fromType,
        const DataType toType
    ){
        return DataTypes::String::Concat(
            allocator,
            "Invalid conversion from ",
            DataTypeToStringDictionary.Get(fromType),
            " to ",
            DataTypeToStringDictionary.Get(toType)
        );
    }
}
