#include "../../include/Visitor.h"
#include "../../include/Statements.h"
#include "../../../Systemic/include/Converter.h"
#include "../../include/ErrorListener.h"

namespace QueryPipeline{

  antlrcpp::Any SQLVisitorImplementation::visitCreateTableStatement(SQLParser::CreateTableStatementContext *context){
    auto* statement = new Statements::CreateTableStatement();

    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    for (const auto columnContext: context->addColumn()) {
      const auto column = std::any_cast<Statements::NewColumn*>(visit(columnContext));
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

    incrementStatement->seed = Converter<uint8_t>::Stoi(context->seed->getText());
    incrementStatement->incrementFactor = Converter<uint8_t>::Stoi(context->increment->getText());

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
      throw SyntaxError("Cannot set a primary key with a default value.", CreatePositionErrorMessage(context));

    if (context->primaryKey() && context->NULL_())
      throw SyntaxError("Cannot set a primary key with default value NULL.", CreatePositionErrorMessage(context));


    return new Statements::NewColumn{
      .name = std::any_cast<Statements::ColumnName>(visit(context->columnName())),
      .type = std::any_cast<Statements::ColumnType>(visit(context->dataType())),
      .identity = key,
      .defaultValue =  context->defaultValue()
              ? std::any_cast<Value>(visit(context->defaultValue()))
              : Value::Null(),
      .isPrimaryKey = isPrimaryKey,
      .isNullable = isNullable,
    };
  }
}