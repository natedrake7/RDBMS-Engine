#include "Visitor.h"
#include "../Statements/Statements.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../ErrorListener/ErrorListener.h"

namespace QueryPipeline{

  antlrcpp::Any SQLVisitorImplementation::visitCreateTableStatement(SQLParser::CreateTableStatementContext *context){
    auto* statement = new Statements::CreateTableStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    for (const auto columnContext: context->addColumn()) {
      const auto column = std::any_cast<Statements::AddColumn*>(visit(columnContext));
      statement->columns.push_back(column);
    }

    if (context->primaryKeyConstraint())
      statement->constraint = std::any_cast<Statements::PrimaryKeyConstraint*>(visit(context->primaryKeyConstraint()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitPrimaryKey(SQLParser::PrimaryKeyContext *context){
    if(context->autoIncrementKey())
      return visit(context->autoIncrementKey());

    return nullptr;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAutoIncrementKey(SQLParser::AutoIncrementKeyContext *context){
    auto* incrementStatement = new Statements::Identity();

    incrementStatement->seed = SafeConverter<uint8_t>::SafeStoi(context->seed->getText());
    incrementStatement->incrementFactor = SafeConverter<uint8_t>::SafeStoi(context->increment->getText());

    return incrementStatement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAddColumn(SQLParser::AddColumnContext *context){
    const bool isPrimaryKey = (context->primaryKey()) != nullptr;

    Statements::Identity* key = (isPrimaryKey && context->primaryKey()->autoIncrementKey())
                    ? std::any_cast<Statements::Identity*>(visit(context->primaryKey()))
                    : nullptr;

    const bool isNullable = ((!context->NULL_() && ! context->NOT() && !isPrimaryKey)
                              || (context->NULL_() && !context->NOT()) && !isPrimaryKey);

    if (context->primaryKey() && context->defaultValue())
      throw SyntaxError("Cannot set a primary key with a default value.");

    if (context->primaryKey() && context->NULL_())
      throw SyntaxError("Cannot set a primary key with default value NULL.");


    return new Statements::AddColumn{
      .name = std::any_cast<Statements::ColumnName>(visit(context->columnName())),
      .type = std::any_cast<Statements::ColumnType>(visit(context->dataType())),
      .autoIncrementKey = key,
      .defaultValue =  context->defaultValue()
              ? std::any_cast<Field>(visit(context->defaultValue()))
              : Field(nullptr),
      .isPrimaryKey = isPrimaryKey,
      .isNullable = isNullable,
    };
  }
}