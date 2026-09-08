#pragma once
#include "../../Systemic/include/Converter.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace QueryPipeline::Messages{
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

    static constexpr DataTypes::String DATABASE_ALREADY_EXISTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& name
    ){
        return DataTypes::String::Concat(allocator, "Database", name, " already exists");
    }

    static constexpr DataTypes::String DATABASE_DOES_NOT_EXIST_ON_DROP(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    ){
        return DataTypes::String::Concat(allocator, "Cannot drop.Database: ", name, " does not exist");
    }

    static constexpr DataTypes::String DATABASE_DOES_NOT_EXIST_ON_USE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    ){
        return DataTypes::String::Concat(allocator, "Cannot use.Database: ", name, " does not exist");
    }

    static constexpr DataTypes::String CANNOT_DROP_SYSTEM_DATABASE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    )
    {
        return DataTypes::String::Concat(allocator, "Cannot drop.Database: ", name, " is a system database");
    }

    static constexpr DataTypes::StringView FAILED_TO_ADD_VARIABLE = "Failed to add variable";

    static constexpr DataTypes::StringView FAILED_TO_RETRIEVE_USER_SESSION = "Failed to retrieve user session";
    static constexpr DataTypes::StringView FAILED_TO_CREATE_USER = "Failed to create user";


    static constexpr DataTypes::StringView USE_DATABASE_SUCCESS = "Database changed successfully";
    static constexpr DataTypes::StringView USE_DATABASE_FAIL = "Failed to use Database";

    static DataTypes::String INSERT_ROWS_FROM_CHILD_QUERY(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter::IntToStr<Int>(count, allocator);
        return DataTypes::String::Concat(allocator, "Inserted ", parsedInt, " rows from child query");
    }
    static DataTypes::String INSERT_ROWS_FROM_FIELDS(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter::IntToStr<Int>(count, allocator);
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
            SQL_TYPES_NAMES[static_cast<Int>(fromType)],
            " to ",
            SQL_TYPES_NAMES[static_cast<Int>(toType)]
        );
    }

    static constexpr DataTypes::StringView MISSING_TABLE_IN_JOIN = "Missing table in join condition";

    /**
     * Identity Error Messages
     */
    static constexpr DataTypes::StringView INVALID_INCREMENT_FACTOR = "Increment Factor must be greater than zero";

    static constexpr DataTypes::StringView DOT = ".";

    static DataTypes::String INVALID_TABLE(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Table", name, " does not exist");
    }

    static DataTypes::String TABLE_ALREADY_EXISTS(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Table", name, " already exists");
    }

    static DataTypes::String SCHEMA_DOES_NOT_EXIST(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Schema", name, " does not exist");
    }

    static DataTypes::String SCHEMA_ALREADY_EXISTS(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Schema", name, " already exists");
    }

    static DataTypes::String DATATYPE_DOES_NOT_EXIST(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Datatype", name, " does not exist");
    }

    static constexpr DataTypes::StringView INVALID_DECIMAL_DECLARATION = "Decimal type requires precision and scale to be set correctly";

    static constexpr DataTypes::StringView MULTIPLE_PRIMARY_KEYS = "Cannot have multiple primary keys defined. Consider declaring a composite key";

    static constexpr DataTypes::StringView PRIMARY_KEY_AND_CONSTRAINT_DECLARED = "Cannot have a primary key and a constraint declared";

    static constexpr DataTypes::StringView MAX_NUMBER_OF_COLUMNS_EXCEEDED = "Column count cannot exceed 128";

    static constexpr DataTypes::StringView JOIN_WITH_NO_BASE_TABLE_SELECT = "Cannot join with no base table selected";

    static constexpr DataTypes::StringView DUPLICATE_RESULT_COLUMN_NAMES = "Duplicate column names in result set. Consider using aliases to disambiguate";

    static constexpr DataTypes::StringView INVALID_WHERE_CLAUSE = "Where expression must be either a logical or a binary expression";

    static DataTypes::String CLAUSE_CANNOT_BE_EVALUATED_TO_BOOLEAN(const ::Memory::IAllocator* allocator, const DataTypes::StringView& expressionType){
        return DataTypes::String::Concat(allocator, "Expression of type: ", expressionType, " cannot be evaluated to type: Bool");
    }

    static DataTypes::String COLUMN_DOES_NOT_ALLOW_NULLS(const ::Memory::IAllocator* allocator, const DataTypes::String& columnName){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not allow null values");
    }

    static DataTypes::String CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const DataTypes::StringView& valueType
    ){
        return DataTypes::String::Concat(allocator, "Cannot update column: ", columnName, "of type: ", columnType, " with datatype: ", valueType);
    }

    static DataTypes::String CANNOT_ALTER_COLUMN_TO_TYPE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const DataTypes::StringView& valueType
    ){
        return DataTypes::String::Concat(allocator, "Cannot alter column: ", columnName, "of type: ", columnType, " to type: ", valueType);
    }

    static DataTypes::String CANNOT_ALTER_COLUMN_TO_NEW_SIZE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const Int valueSize
    ){
        const auto valueSizeStr = Converter::IntToStr<Int>(valueSize, allocator);
        return DataTypes::String::Concat(allocator, "Cannot alter column: ", columnName, "of type: ", columnType, " to new size: ", valueSizeStr);
    }

    static constexpr DataTypes::StringView INSERT_STATEMENT_INVALID_NUMBER_OF_ARGUMENTS_ON_SUB_SELECT =
        "Invalid number of arguments specified on select statement";

    static constexpr DataTypes::StringView NO_TABLE_SPECIFIED = "No table specified";

    static constexpr DataTypes::String COLUMN_DOES_NOT_EXIST_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& tableName,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not exist on table: ", tableName);
    }

    static constexpr DataTypes::String COLUMN_DOES_NOT_EXIST_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not exist on any table");
    }

    static DataTypes::String COLUMN_ALREADY_EXISTS_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& tableName,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " already exists on table: ", tableName);
    }

    static constexpr DataTypes::StringView IDENTITY_COLUMN_ON_INSERT = "Cannot specify an identity column for insert";

    static constexpr DataTypes::String INDEX_EXISTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& indexName
    ){
        return DataTypes::String::Concat(allocator, "Index: ", indexName, " already exists");
    }

    static constexpr DataTypes::StringView DEFAULT_VALUE_NULL_ON_NOT_NULL_COLUMN = "Default value cannot be null for a not null column";

    static DataTypes::String INVALID_COLUMN_TYPE_SPECIFIED(const ::Memory::IAllocator* allocator, const DataTypes::String& columnName, const DataTypes::StringView& columnType){
        return DataTypes::String::Concat(allocator, "Invalid column type: ", columnType, " specified for column: ", columnName);
    }

    static DataTypes::String CANNOT_DROP_COLUMN_HAS_CONSTRAINTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::String& constraintName
    ){
        return DataTypes::String::Concat(allocator, "Cannot drop column: ", columnName, " as it is referenced by constraint: ", constraintName);
    }

    static constexpr DataTypes::StringView UNKNOWN_OPERATION = "Unknown operation";

    static DataTypes::String INVALID_OPERATION_ON_DATATYPES(
        const ::Memory::IAllocator* allocator,
        const DataType leftType,
        const DataType rightType
    ){
        return DataTypes::String::Concat(
            allocator,
            "Invalid operation on datatypes: ",
            SQL_TYPES_NAMES[static_cast<Int>(leftType)],
            " and ",
            SQL_TYPES_NAMES[static_cast<Int>(rightType)]
        );
    }

    static constexpr DataTypes::StringView INVALID_NUMBER_OF_ARGUMENTS_ON_BRANCH_EXPRESSION = "Invalid number of arguments specified on branch expression";

    static DataTypes::String INVALID_EXPRESSION_TYPE_FOR_BRANCH_EXPRESSION(const ::Memory::IAllocator* allocator, const DataType type){
        return DataTypes::String::Concat(
            allocator,
            "Invalid expression type: ",
            SQL_TYPES_NAMES[static_cast<Int>(type)],
            " specified for branch expression"
        );
    }

    static constexpr DataTypes::StringView INVALID_BRANCH_EXPRESSION_RESULT_TYPE = "Branching Expression Result types cannot be coerced to datatype";

    static DataTypes::String INVALID_COLUMN_ALIAS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(
            allocator,
            "No table was specified but column with name: ",
            columnName,
            " was specified."
        );
    }

    static DataTypes::String INVALID_TABLE_ALIAS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& tableName
    ) {
        return DataTypes::String::Concat(
            allocator,
            "Alias: ",
            tableName,
            " does not exist in the statement"
        );
    }

    static DataTypes::String INVALID_COLUMN_NAME(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnAlias
    ) {
        return DataTypes::String::Concat(
            allocator,
            "Column: ",
            columnAlias,
            " does not exist in the statement"
        );
    }

    static DataTypes::String AMBIGUOUS_COLUMN_NAME(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnAlias
    ) {
        return DataTypes::String::Concat(
            allocator,
            "Ambiguous column name: ",
            columnAlias
        );
    }

    static constexpr DataTypes::StringView UNEXPECTED_ERROR = "Unexpected Error Occurred. Please contact support.";

    static constexpr DataTypes::StringView WILDCARD_USED_ON_NON_SELECT = "WildCard was used but statement is not of type SELECT";

    static DataTypes::String INVALID_VARIABLE(const ::Memory::IAllocator* allocator, const DataTypes::String& variableName){
        return DataTypes::String::Concat(allocator, "Variable: ", variableName, " was not declared in this scope.");
    }

    static constexpr DataTypes::StringView EMPTY_JSON_PATH = "JSON data access path cannot be empty.";

    static DataTypes::String INVALID_JSON_PATH(const ::Memory::IAllocator* allocator, const DataTypes::StringView& path){
        return DataTypes::String::Concat(allocator, "Invalid JSON data access path: ", path, ". Last access must always be scalar");
    }

    static constexpr DataTypes::StringView INVALID_JSON_ACCESSOR_TYPE = "Invalid json accessor type. Intermediate accessors must always be ->";

    static constexpr DataTypes::StringView WILDCARD_NOT_ALLOWED_IN_ORDER_BY = "Wildcard * is not allows in order by clause";

    static constexpr DataTypes::StringView ORDER_BY_NULL_VALUE = "NULL cannot be used in order by clause";

    static constexpr DataTypes::String ORDER_BY_INTEGRAL_INVALID_VALUE(
        const ::Memory::IAllocator* allocator,
        const Int index
    ){
        const auto parsedInt = Converter::IntToStr<Int>(index, allocator);
        return DataTypes::String::Concat(
            allocator, "Invalid value: ", parsedInt, " specified in order by clause. Must be greater than or equal to 1 and smaller than the projection count"
        );
    }
}
