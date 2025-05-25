#include "Visitor.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../Statements/Statements.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitSqlStatement(SQLParser::SqlStatementContext *context)  {
    if (context->selectStatement())
      return visit(context->selectStatement());
    if (context->createDbStatement())
      return visit(context->createDbStatement());
    if (context->dropDbStatement())
      return visit(context->dropDbStatement());
    if (context->insertStatement())
      return visit(context->insertStatement());
    if (context->createTableStatement())
      return visit(context->createTableStatement());
    if (context->createSchemaStatement())
      return visit(context->createSchemaStatement());
    if (context->deleteStatement())
      return visit(context->deleteStatement());
    
    return nullptr;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) {
    auto* statement = new Statements::CreateDbStatement();

    if (context->IDENTIFIER())
      statement->name = context->IDENTIFIER()->getText();

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDropDbStatement(SQLParser::DropDbStatementContext *context) {
    auto* statement = new Statements::DropDbStatement();

    if (context->IDENTIFIER())
      statement->name = context->IDENTIFIER()->getText();

    return statement;
  }

antlrcpp::Any SQLVisitorImplementation::visitSelectStatement(SQLParser::SelectStatementContext *ctx) {
    auto* statement = new Statements::SelectStatement();

    if (!ctx->WILDCARD()) {
      // Visit columnList and get column names
      auto colCtx = ctx->columnList();
      for (const auto col : colCtx->columnName())
        statement->columns.push_back(col->getText());
    }
    else
      statement->columns = {"*"};

    statement->table = std::any_cast<Statements::TableName*>(visit(ctx->tableName()));

    if (const auto& whereClause = ctx->whereClause();whereClause != nullptr)
      statement->where = std::any_cast<Statements::WhereClause>(visit(whereClause));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitWhereClause(SQLParser::WhereClauseContext *context){
    Statements::WhereClause where;
    where.expression = std::any_cast<Expressions::Expression*>(visitExpression(context->expression()));
    return where;
  }

  antlrcpp::Any SQLVisitorImplementation::visitExpression(SQLParser::ExpressionContext *context){
    return visit(context->orExpression());
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
    if (context->STRING()) {
      const auto& str = context->STRING()->getText();

      return Field(AdditionalLibraries::RemoveQuotesFromString(str), 0);
    }

    if (context->NUMBER()) {
      const auto number = SafeConverter<int64_t>::SafeStoi(context->NUMBER()->getText());

      return Field(number, 0);
    }
    //datetime obj
    if (context->getDate())
      return Field(DataTypes::DateTime::Now(), 0);

    throw invalid_argument("Invalid value specified");
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrExpression(SQLParser::OrExpressionContext *context){
    auto* expression = std::any_cast<Expressions::Expression*>(visit(context->andExpression(0)));

    for (size_t i = 1; i < context->andExpression().size(); i++) {
      auto* right = std::any_cast<Expressions::Expression*>(visit(context->andExpression(i)));
      
      expression = new Expressions::Expression(Expressions::ExpressionType::Or, expression, right);  // assuming you have a class like this
    }

    return expression;
  }

antlrcpp::Any SQLVisitorImplementation::visitAndExpression(SQLParser::AndExpressionContext *context) {
    auto* expression = std::any_cast<Expressions::Expression*>(visit(context->predicate(0)));

    for (size_t i = 1; i < context->predicate().size(); i++) {
      auto* right = std::any_cast<Expressions::Expression*>(visit(context->predicate(i)));
      
      expression = new Expressions::Expression(Expressions::ExpressionType::And, expression, right);  // assuming you have a class like this
    }

    return expression;
  }
  antlrcpp::Any SQLVisitorImplementation::visitPredicate(SQLParser::PredicateContext *context){
    if (context->expression())
      return visit(context->expression());

    return new Expressions::Expression{
      Expressions::ExpressionType::Predicate,
      nullptr,
      nullptr,
      context->columnName()->getText(),
      context->op->getText(),
      std::any_cast<Field>(visit(context->literalValue()))
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitInsertStatement(SQLParser::InsertStatementContext *context){
    auto* statement = new Statements::InsertStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));
    
    const auto columns = visit(context->columnList());

    statement->columns = std::any_cast<std::vector<string>>(columns);

    const auto values = visit(context->literalValueList());

    statement->values = std::any_cast<std::vector<Field>>(values);

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValueList(SQLParser::LiteralValueListContext *context){
    vector<Field> values;
    
    for (const auto& literalValue : context->literalValue())
       values.emplace_back(std::any_cast<Field>(visit(literalValue)));

    return values;
  }

  antlrcpp::Any SQLVisitorImplementation::visitGetDate(SQLParser::GetDateContext *context){
      return {};
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateTableStatement(SQLParser::CreateTableStatementContext *context){
    auto* statement = new Statements::CreateTableStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    const auto columns = context->addColumn();
    for (const auto columnContext: columns) {
      const auto column = std::any_cast<Statements::AddColumn>(visit(columnContext));
      statement->columns.push_back(column);
    }

    if (context->primaryKeyConstraint())
      statement->constraint = std::any_cast<Statements::PrimaryKeyConstraint*>(visit(context->primaryKeyConstraint()));

    return statement;
  }

antlrcpp::Any SQLVisitorImplementation::visitDataType(SQLParser::DataTypeContext *context) {
    if (context->varcharType())
      return visit(context->varcharType());
    if (context->nvarcharType())
      return visit(context->varcharType());

    const auto& text = context->getText();

    return Statements::ColumnType{
      .name = AdditionalLibraries::NormalizeString(text),
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitPrimaryKey(SQLParser::PrimaryKeyContext *context){
    return {};
  }

  antlrcpp::Any SQLVisitorImplementation::visitAddColumn(SQLParser::AddColumnContext *context){
    const bool isPrimaryKey = (context->primaryKey()) ? true : false;
    const bool isNullable = ((context->NULL_() && !context->NOT()) && !isPrimaryKey);

    return Statements::AddColumn{
      .name = std::any_cast<string>(visit(context->columnName())),
      .type = std::any_cast<Statements::ColumnType>(visit(context->dataType())),
      .isPrimaryKey = isPrimaryKey,
      .isNullable = isNullable,
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitVarcharType(SQLParser::VarcharTypeContext *context){
    const auto& number = context->NUMBER();

    return Statements::ColumnType{
      .name = QueryPipeline::String,
      .size = number ? SafeConverter<int64_t>::SafeStoi(number->getText()) : -1,
      .beforeFraction =  -1,
      .afterFraction = -1
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitNvarcharType(SQLParser::NvarcharTypeContext *context){
    const auto& number = context->NUMBER();

    return Statements::ColumnType{
        .name = QueryPipeline::UnicodeString,
        .size = number ? SafeConverter<int64_t>::SafeStoi(number->getText()) : -1,
        .beforeFraction =  -1,
        .afterFraction = -1
      };
  }

  antlrcpp::Any SQLVisitorImplementation::visitDecimalType(SQLParser::DecimalTypeContext *context){
    return Statements::ColumnType{
      .size = 0,
      .beforeFraction = SafeConverter<int64_t>::SafeStoi(context->beforePoint->getText()),
      .afterFraction = SafeConverter<int64_t>::SafeStoi(context->afterPoint->getText()),
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitPrimaryKeyConstraint(SQLParser::PrimaryKeyConstraintContext *context){
    auto* statement = new Statements::PrimaryKeyConstraint();

    if (context->constraintName)
      statement->name = context->constraintName->getText();

    auto colCtx = context->columnList();
    for (const auto col : colCtx->columnName())
      statement->columns.push_back(col->getText());

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateSchemaStatement(SQLParser::CreateSchemaStatementContext *context){

    auto* statement = new Statements::CreateSchemaStatement();

    statement->name = context->IDENTIFIER()->getText();
    
    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDeleteStatement(SQLParser::DeleteStatementContext *context){
    auto* statement = new Statements::DeleteStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    if (context->whereClause())
      statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

    return statement;
  }


  antlrcpp::Any SQLVisitorImplementation::visitColumnName(SQLParser::ColumnNameContext *context){
    return context == nullptr ? "" : context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitTableName(SQLParser::TableNameContext *context) {
    auto* statement = new Statements::TableName();
    
    if (context->schemaName)
      statement->schema = context->schemaName->getText();

    statement->name = context->name->getText();
    
    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDbName(SQLParser::DbNameContext *context){
      return context == nullptr ? "" : context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitColumnList(SQLParser::ColumnListContext *context){
      std::vector<std::string> columns;
    
      for (const auto &columnName : context->columnName())
        columns.push_back(columnName->getText());

      return columns; // Return vector of column names
  }
}
