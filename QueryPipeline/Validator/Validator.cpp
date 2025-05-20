#include "Validator.h"

#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../Visitor/Visitor.h"
#include "../../Server/Server.h"

namespace QueryPipeline {
  void Validator::Validate(const CreateDbStatement& statement){
    if (!Server::ServerInstance::Get().DatabaseExists(statement.name))
      return;

    throw runtime_error("Database " + statement.name + " already exists");
  }

  void Validator::Validate(const DropDbStatement& statement){
    const auto database = Server::ServerInstance::Get().SelectDatabases(statement.name);

    if (database.name.empty())
      throw runtime_error("Cannot drop: " + statement.name + ". Database" + statement.name + " does not exist");

    if (database.isSystem)
      throw runtime_error("Cannot drop: a system database");
  }

  void  Validator::Validate(SelectStatement& statement) {
    //get only table needed. if joins occur get them all
    const auto tables = Server::ServerInstance::Get().SelectTables("masterDb");

    const Headers::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == statement.table) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr == nullptr)
      throw runtime_error("Table " + statement.table + " does not exist");

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary("masterDb", headerPtr->name);

    for (const auto& selectColumn : statement.columns) {
      if (Headers::ColumnHeader header ;columnsDict.TryGetValue(selectColumn, header)) {
        statement.columnIndices.emplace_back(header.tablePosition);
        continue;
      }
      
      throw runtime_error("Column " + selectColumn + " does not exist");
    }

    if (statement.where.expression == nullptr)
      return;

    Validator::Validate(statement.where.expression, columnsDict);
  }

  void Validator::Validate(Expression *expression, const Dictionary<string, Headers::ColumnHeader>& columnsDictionary){

    if (expression->type != ExpressionType::Predicate
      && expression->left != nullptr
      && expression->right != nullptr) {
      Validator::Validate(expression->left, columnsDictionary);
      Validator::Validate(expression->right, columnsDictionary);

      return;
    }

    Headers::ColumnHeader header;
    if (!columnsDictionary.TryGetValue(expression->column, header))
      throw runtime_error("Column " + expression->column + " does not exist");

    expression->columnIndex = header.tablePosition;
  }

  void Validator::Validate(InsertStatement &statement){
    const auto tables = Server::ServerInstance::Get().SelectTables("masterDb");

    const Headers::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == statement.tableName) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr == nullptr)
      throw runtime_error("Table " + statement.tableName + " does not exist");

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary("masterDb", headerPtr->name);

    if (statement.columns.size() != statement.values.size())
      throw runtime_error("Invalid number of arguments supplied");

    for (int i = 0;i < statement.columns.size(); i++) {
      const auto& column = statement.columns[i];

     Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(column, header))
        throw runtime_error("Column " + column + " does not exist on table: " + headerPtr->name);

      auto& value = statement.values.at(i);
      Validator::Validate(&value, header);
    }

    for (const auto&[columnName, header]:  columnsDict) {
      bool columnExistsInStatement = false;
      
        for (const auto& statementColumn: statement.columns) {
          if (columnName != statementColumn)
            continue;
          
          columnExistsInStatement = true;
          break;
        }

      if (!columnExistsInStatement && !header.isNullable)
        throw invalid_argument("Column " + columnName + " does not allow NULLS. Insert fails");

      if (columnExistsInStatement)
        continue;

      statement.values.emplace_back(nullptr, header.tablePosition);
    }
  }

  void Validator::Validate(Field* field, const Headers::ColumnHeader &header){
    switch (const auto& columnType = ColumnTypesDictionary.Get(header.dataType)) {
      case ColumnType::TinyInt: {
        const auto value = SafeConverter<int8_t>::SafeStoi(field->GetBigInt());
        field->SetData(value);
        break;
      }
      case ColumnType::SmallInt: {
        const auto value = SafeConverter<int16_t>::SafeStoi(field->GetBigInt());
        field->SetData(value);
        break;
      }
      case ColumnType::Int:{
        const auto value = SafeConverter<int32_t>::SafeStoi(field->GetBigInt());
        field->SetData(value);
        break;
      }
      case ColumnType::BigInt:
        SafeConverter<int64_t>::SafeStoi(field->GetBigInt());
        break;
      case ColumnType::String:
      case ColumnType::UnicodeString:
        if (columnType != field->GetType())
          throw runtime_error("Column " + header.name + " has different data type than specified");
        break;
      case ColumnType::Bool: {
        const auto value = SafeConverter<bool>::SafeStoi(field->GetBigInt());
        field->SetData(value);
        break;
      }
      case ColumnType::DateTime: {
        const auto datetime = field->GetDateTime();
        if (!DataTypes::DateTime::ValidateDate(datetime))
            throw invalid_argument("failed to validate date");

        break;
      }
      case ColumnType::Decimal:

        break;
      default:
      case ColumnType::ColumnTypeCount:
        throw invalid_argument("Invalid column type");
    }

    field->SetColumnIndex(header.tablePosition);
  }

}