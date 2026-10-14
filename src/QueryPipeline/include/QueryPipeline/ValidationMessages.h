#pragma once
#include <Systemic/Converter.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>

namespace QueryPipeline::Messages{
    inline DataTypes::String ADDED_VARIABLE(
        const DataTypes::StringView& name,
        const ::Memory::IAllocator* allocator
    ){ return DataTypes::String::Concat(allocator, "Added variable: ", name); };
    inline DataTypes::String FAILED_TO_GET_ROLE(
        const DataTypes::StringView& role,
        const ::Memory::IAllocator* allocator
    ){
        return DataTypes::String::Concat(allocator, "Failed to get role: ", role);
    }

    inline constexpr DataTypes::String DATABASE_ALREADY_EXISTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& name
    ){
        return DataTypes::String::Concat(allocator, "Database", name, " already exists");
    }

    inline constexpr DataTypes::String DATABASE_DOES_NOT_EXIST_ON_DROP(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    ){
        return DataTypes::String::Concat(allocator, "Cannot drop.Database: ", name, " does not exist");
    }

    inline constexpr DataTypes::String DATABASE_DOES_NOT_EXIST_ON_USE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    ){
        return DataTypes::String::Concat(allocator, "Cannot use.Database: ", name, " does not exist");
    }

    inline constexpr DataTypes::String CANNOT_DROP_SYSTEM_DATABASE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name
    )
    {
        return DataTypes::String::Concat(allocator, "Cannot drop.Database: ", name, " is a system database");
    }

    inline constexpr DataTypes::StringView FAILED_TO_ADD_VARIABLE = "Failed to add variable";

    inline constexpr DataTypes::StringView FAILED_TO_RETRIEVE_USER_SESSION = "Failed to retrieve user session";
    inline constexpr DataTypes::StringView FAILED_TO_CREATE_USER = "Failed to create user";


    inline constexpr DataTypes::StringView USE_DATABASE_SUCCESS = "Database changed successfully";
    inline constexpr DataTypes::StringView USE_DATABASE_FAIL = "Failed to use Database";

    inline DataTypes::String INSERT_ROWS_FROM_CHILD_QUERY(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter::IntToStr<Int>(count, allocator);
        return DataTypes::String::Concat(allocator, "Inserted ", parsedInt, " rows from child query");
    }
    inline DataTypes::String INSERT_ROWS_FROM_FIELDS(const Int count, const ::Memory::IAllocator* allocator){
        const auto parsedInt = Converter::IntToStr<Int>(count, allocator);
        return DataTypes::String::Concat(allocator, "Inserted ", parsedInt, " rows from fields");
    }

    inline constexpr DataTypes::StringView FAILED_TO_FETCH_USER_SESSION = "Failed to fetch user session";
    inline constexpr DataTypes::StringView EMPTY_USERNAME = "username cannot be empty";
    inline constexpr DataTypes::StringView EMPTY_PASSWORD = "password cannot be empty";
    inline constexpr DataTypes::StringView EMPTY_ROLE = "role cannot be empty";
    inline constexpr DataTypes::StringView FAILED_TO_FETCH_ROLE = "Failed to find given role";
    inline constexpr DataTypes::StringView USER_ALREADY_EXISTS = "User with username already exists";
    inline constexpr DataTypes::StringView USER_DOES_NOT_EXIST = "User does not exist";

    inline DataTypes::String INVALID_DATATYPE_CONVERSION_MESSAGE(
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

    inline constexpr DataTypes::StringView MISSING_TABLE_IN_JOIN = "Missing table in join condition";

    /**
     * Identity Error Messages
     */
    inline constexpr DataTypes::StringView INVALID_INCREMENT_FACTOR = "Increment Factor must be greater than zero";

    inline constexpr DataTypes::StringView DOT = ".";

    inline DataTypes::String INVALID_TABLE(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Table", name, " does not exist");
    }

    inline DataTypes::String TABLE_ALREADY_EXISTS(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Table", name, " already exists");
    }

    inline DataTypes::String SCHEMA_DOES_NOT_EXIST(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Schema", name, " does not exist");
    }

    inline DataTypes::String SCHEMA_ALREADY_EXISTS(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Schema", name, " already exists");
    }

    inline DataTypes::String DATATYPE_DOES_NOT_EXIST(const ::Memory::IAllocator* allocator, const DataTypes::String& name){
        return DataTypes::String::Concat(allocator, "Datatype", name, " does not exist");
    }

    inline constexpr DataTypes::StringView INVALID_DECIMAL_DECLARATION = "Decimal type requires precision and scale to be set correctly";

    inline constexpr DataTypes::StringView MULTIPLE_PRIMARY_KEYS = "Cannot have multiple primary keys defined. Consider declaring a composite key";

    inline constexpr DataTypes::StringView PRIMARY_KEY_AND_CONSTRAINT_DECLARED = "Cannot have a primary key and a constraint declared";

    inline constexpr DataTypes::StringView MAX_NUMBER_OF_COLUMNS_EXCEEDED = "Column count cannot exceed 128";

    inline constexpr DataTypes::StringView JOIN_WITH_NO_BASE_TABLE_SELECT = "Cannot join with no base table selected";

    inline constexpr DataTypes::StringView DUPLICATE_RESULT_COLUMN_NAMES = "Duplicate column names in result set. Consider using aliases to disambiguate";

    inline constexpr DataTypes::StringView INVALID_WHERE_CLAUSE = "Where expression must be either a logical or a binary expression";

    inline DataTypes::String CLAUSE_CANNOT_BE_EVALUATED_TO_BOOLEAN(const ::Memory::IAllocator* allocator, const DataTypes::StringView& expressionType){
        return DataTypes::String::Concat(allocator, "Expression of type: ", expressionType, " cannot be evaluated to type: Bool");
    }

    inline DataTypes::String COLUMN_DOES_NOT_ALLOW_NULLS(const ::Memory::IAllocator* allocator, const DataTypes::String& columnName){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not allow null values");
    }

    inline DataTypes::String CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const DataTypes::StringView& valueType
    ){
        return DataTypes::String::Concat(allocator, "Cannot update column: ", columnName, "of type: ", columnType, " with datatype: ", valueType);
    }

    inline DataTypes::String CANNOT_ALTER_COLUMN_TO_TYPE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const DataTypes::StringView& valueType
    ){
        return DataTypes::String::Concat(allocator, "Cannot alter column: ", columnName, "of type: ", columnType, " to type: ", valueType);
    }

    inline DataTypes::String CANNOT_ALTER_COLUMN_TO_NEW_SIZE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::StringView& columnType,
        const Int valueSize
    ){
        const auto valueSizeStr = Converter::IntToStr<Int>(valueSize, allocator);
        return DataTypes::String::Concat(allocator, "Cannot alter column: ", columnName, "of type: ", columnType, " to new size: ", valueSizeStr);
    }

    inline constexpr DataTypes::StringView INSERT_STATEMENT_INVALID_NUMBER_OF_ARGUMENTS_ON_SUB_SELECT =
        "Invalid number of arguments specified on select statement";

    inline constexpr DataTypes::StringView NO_TABLE_SPECIFIED = "No table specified";

    inline constexpr DataTypes::String COLUMN_DOES_NOT_EXIST_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& tableName,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not exist on table: ", tableName);
    }

    inline constexpr DataTypes::String COLUMN_DOES_NOT_EXIST_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " does not exist on any table");
    }

    inline DataTypes::String COLUMN_ALREADY_EXISTS_ON_TABLE(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& tableName,
        const DataTypes::String& columnName
    ){
        return DataTypes::String::Concat(allocator, "Column: ", columnName, " already exists on table: ", tableName);
    }

    inline constexpr DataTypes::StringView IDENTITY_COLUMN_ON_INSERT = "Cannot specify an identity column for insert";

    inline constexpr DataTypes::String INDEX_EXISTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& indexName
    ){
        return DataTypes::String::Concat(allocator, "Index: ", indexName, " already exists");
    }

    inline constexpr DataTypes::StringView DEFAULT_VALUE_NULL_ON_NOT_NULL_COLUMN = "Default value cannot be null for a not null column";

    inline DataTypes::String INVALID_COLUMN_TYPE_SPECIFIED(const ::Memory::IAllocator* allocator, const DataTypes::String& columnName, const DataTypes::StringView& columnType){
        return DataTypes::String::Concat(allocator, "Invalid column type: ", columnType, " specified for column: ", columnName);
    }

    inline DataTypes::String CANNOT_DROP_COLUMN_HAS_CONSTRAINTS(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnName,
        const DataTypes::String& constraintName
    ){
        return DataTypes::String::Concat(allocator, "Cannot drop column: ", columnName, " as it is referenced by constraint: ", constraintName);
    }

    inline constexpr DataTypes::StringView UNKNOWN_OPERATION = "Unknown operation";

    inline DataTypes::String INVALID_OPERATION_ON_DATATYPES(
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

    inline constexpr DataTypes::StringView INVALID_NUMBER_OF_ARGUMENTS_ON_BRANCH_EXPRESSION = "Invalid number of arguments specified on branch expression";

    inline DataTypes::String INVALID_EXPRESSION_TYPE_FOR_BRANCH_EXPRESSION(const ::Memory::IAllocator* allocator, const DataType type){
        return DataTypes::String::Concat(
            allocator,
            "Invalid expression type: ",
            SQL_TYPES_NAMES[static_cast<Int>(type)],
            " specified for branch expression"
        );
    }

    inline constexpr DataTypes::StringView INVALID_BRANCH_EXPRESSION_RESULT_TYPE = "Branching Expression Result types cannot be coerced to datatype";

    inline DataTypes::String INVALID_COLUMN_ALIAS(
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

    inline DataTypes::String INVALID_TABLE_ALIAS(
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

    inline DataTypes::String INVALID_COLUMN_NAME(
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

    inline DataTypes::String AMBIGUOUS_COLUMN_NAME(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& columnAlias
    ) {
        return DataTypes::String::Concat(
            allocator,
            "Ambiguous column name: ",
            columnAlias
        );
    }

    inline constexpr DataTypes::StringView UNEXPECTED_ERROR = "Unexpected Error Occurred. Please contact support.";

    inline constexpr DataTypes::StringView WILDCARD_USED_ON_NON_SELECT = "WildCard was used but statement is not of type SELECT";

    inline DataTypes::String INVALID_VARIABLE(const ::Memory::IAllocator* allocator, const DataTypes::String& variableName){
        return DataTypes::String::Concat(allocator, "Variable: ", variableName, " was not declared in this scope.");
    }

    inline constexpr DataTypes::StringView EMPTY_JSON_PATH = "JSON data access path cannot be empty.";

    inline DataTypes::String INVALID_JSON_PATH(const ::Memory::IAllocator* allocator, const DataTypes::StringView& path){
        return DataTypes::String::Concat(allocator, "Invalid JSON data access path: ", path, ". Last access must always be scalar");
    }

    inline constexpr DataTypes::StringView INVALID_JSON_ACCESSOR_TYPE = "Invalid json accessor type. Intermediate accessors must always be ->";

    inline constexpr DataTypes::StringView WILDCARD_NOT_ALLOWED_IN_ORDER_BY = "Wildcard * is not allows in order by clause";

    inline constexpr DataTypes::StringView ORDER_BY_NULL_VALUE = "NULL cannot be used in order by clause";

    inline constexpr DataTypes::String ORDER_BY_INTEGRAL_INVALID_VALUE(
        const ::Memory::IAllocator* allocator,
        const Int index
    ){
        const auto parsedInt = Converter::IntToStr<Int>(index, allocator);
        return DataTypes::String::Concat(
            allocator, "Invalid value: ", parsedInt, " specified in order by clause. Must be greater than or equal to 1 and smaller than the projection count"
        );
    }

    inline constexpr auto TABLE_CREATION_FAILED = DataTypes::StringView("Failed to create table");
    inline constexpr auto TABLE_CREATED_SUCCESSFULLY = DataTypes::StringView("Table created successfully");
}
