#include "Visitor.h"
#include "../../Systemic/Converter/Converter.h"
#include "../../Systemic/Functions/StringFunctions.h"
#include "../ErrorListener/ErrorListener.h"
#include "../Statements/Statements.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitSqlStatement(SQLParser::SqlStatementContext *context)  {
    std::vector<std::any> statements;

    for (const auto& statement: context->statement())
      statements.push_back(this->visit(statement));

    return statements;
  }

  antlrcpp::Any SQLVisitorImplementation::visitStatement(SQLParser::StatementContext *context) {
    if (context->createUserStatement())
      return visit(context->createUserStatement());
    if (context->grantRoleStatement())
      return visit(context->grantRoleStatement());


    if (context->createDbStatement())
      return visit(context->createDbStatement());
    if (context->useDbStatement())
      return visit(context->useDbStatement());
    if (context->dropDbStatement())
      return visit(context->dropDbStatement());

    if (context->selectStatement())
      return visit(context->selectStatement());
    if (context->insertStatement())
      return visit(context->insertStatement());

    if (context->createTableStatement())
      return visit(context->createTableStatement());
    if (context->createSchemaStatement())
      return visit(context->createSchemaStatement());

    if (context->deleteStatement())
      return visit(context->deleteStatement());
    if(context->updateStatement())
      return visit(context->updateStatement());

    if (context->createIndexStatement())
      return visit(context->createIndexStatement());
    if (context->alterTableStatement())
      return visit(context->alterTableStatement());

    if (context->declareVariableStatement())
      return visit(context->declareVariableStatement());
    if (context->setVariableStatement())
      return visit(context->setVariableStatement());

    return nullptr;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) {
    auto* statement = new Statements::CreateDbStatement();

    if (context->identifier())
      statement->name = std::any_cast<std::string>(visit(context->identifier()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDropDbStatement(SQLParser::DropDbStatementContext *context) {
    auto* statement = new Statements::DropDbStatement();

    if (context->identifier())
      statement->name = std::any_cast<std::string>(visit(context->identifier()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitSelectStatement(SQLParser::SelectStatementContext *context) {
    auto* statement = new Statements::SelectStatement();

    if (!context->resultList())
      throw SyntaxError("No arguments specified", CreatePositionErrorMessage(context));

    if (context->top())
      statement->top = std::any_cast<int64_t>(visit(context->top()));

    statement->distinct = context->distinct() != nullptr;

    statement->results = std::any_cast<std::vector<Expressions::Expression*>>(visitResultList(context->resultList()));

    statement->table = (context->tableName() != nullptr)
              ? std::any_cast<Statements::DataSource*>(visit(context->tableName()))
              : nullptr;

    for (auto* join : context->joinStatement())
      statement->joins.push_back(std::any_cast<Statements::JoinStatement*>(visit(join)));

    if (context->whereClause() != nullptr)
      statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

    if(context->orderByStatement())
      statement->orderBy = std::any_cast<Statements::OrderByStatement*>(visit(context->orderByStatement()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitWhereClause(SQLParser::WhereClauseContext *context){
    Statements::WhereClause where;
    const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));

    where.expression = expression;

    return where;
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
    if (context->STRING()) {
      const auto& str = context->STRING()->getText();

      return Value(Functions::String::RemoveQuotesFromString(str), 0);
    }

    if (context->UNICODESTRING()) {
      const auto& str = context->UNICODESTRING()->getText();

      return Value(Functions::String::RemoveQuotesFromString(str), 0);
    }

    if (context->NUMBER()) {
      const auto& numberStr = (context->sign())
            ? context->sign()->getText() + context->NUMBER()->getText()
            : context->NUMBER()->getText();


      const auto number = Converter<int64_t>::Stoi(numberStr);

      return Value(number, 0);
    }

    if (context->DECIMAL_REGEX()) {

      const auto& decimalStr = (context->sign())
            ? context->sign()->getText() + context->DECIMAL_REGEX()->getText()
            : context->DECIMAL_REGEX()->getText();

      return Value(DataTypes::Decimal(decimalStr), 0);
    }

    if (context->NULL_())
      return Value(nullptr, 0);

    if (context->TRUE())
      return Value(true, 0);

    if (context->FALSE())
      return Value(false, 0);

    throw SyntaxError("Invalid value specified" + context->getText(), CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitInsertStatement(SQLParser::InsertStatementContext *context){
    auto* statement = new Statements::InsertStatement();

    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    statement->columns = std::move(this->GetColumnsList(context->columnList()));

    if (context->valuesStatement()) {
      statement->values = std::any_cast<std::vector<Statements::Inserts>>(visit(context->valuesStatement()));
    }

    if (context->selectStatement())
      statement->selectStatement = std::any_cast<Statements::SelectStatement*>(visit(context->selectStatement()));

    if (context->valuesStatement() && context->selectStatement())
      throw SyntaxError("Cannot have both Values Statement and Select Statement in insert", CreatePositionErrorMessage(context));

    if (!context->selectStatement() && !context->valuesStatement())
      throw SyntaxError("Invalid syntax at insert", CreatePositionErrorMessage(context));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValueList(SQLParser::LiteralValueListContext *context){
    vector<Value> values;
    
    for (const auto& literalValue : context->literalValue())
       values.emplace_back(std::any_cast<Value>(visit(literalValue)));

    return values;
  }


antlrcpp::Any SQLVisitorImplementation::visitDataType(SQLParser::DataTypeContext *context) {
    if (context->stringType())
      return visit(context->stringType());

    if (context->uStringType())
      return visit(context->uStringType());

    if (context->decimalType())
      return visit(context->decimalType());

    const auto& text = context->getText();

    return Statements::ColumnType(Functions::String::NormalizeString(text));
  }

  antlrcpp::Any SQLVisitorImplementation::visitStringType(SQLParser::StringTypeContext *context){
    const auto& number = context->NUMBER();

    return Statements::ColumnType(QueryPipeline::String, number ? Converter<int32_t>::Stoi(number->getText()) : -1);
  }

  antlrcpp::Any SQLVisitorImplementation::visitUStringType(SQLParser::UStringTypeContext *context){
    const auto& number = context->NUMBER();

    return Statements::ColumnType(QueryPipeline::UnicodeString, number ? Converter<int32_t>::Stoi(number->getText()) : -1);
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrderColumnList(SQLParser::OrderColumnListContext *context){
    std::vector<Statements::OrderColumn*> orderColumns;

    for (const auto& orderColumn : context->orderColumn())
      orderColumns.push_back(std::any_cast<Statements::OrderColumn*>(visit(orderColumn)));

    return orderColumns;
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrderColumn(SQLParser::OrderColumnContext *context){
    auto* orderColumn = new Statements::OrderColumn();

    const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));

    orderColumn->expression = expression;

    if (!context->order())
      return orderColumn;

    const auto orderStr = std::any_cast<std::string>(visit(context->order()));

    orderColumn->type = Functions::String::NormalizeString(orderStr) == "desc"
          ? OrderType::DESCENDING
          : OrderType::ASCENDING;

    return orderColumn;
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrder(SQLParser::OrderContext *context){
    return context->ASC()
        ? context->ASC()->getText()
        : context->DESC()->getText();
  }

  std::any SQLVisitorImplementation::visitValuesStatement(SQLParser::ValuesStatementContext *context){
    return std::any_cast<std::vector<Statements::Inserts>>(visit(context->valuesList()));
  }

  std::any SQLVisitorImplementation::visitValuesList(SQLParser::ValuesListContext *context){
    std::vector<Statements::Inserts> values;

    values.reserve(context->resultList().size());

    for (const auto& value : context->resultList()) {

      auto resultList = std::any_cast<std::vector<Expressions::Expression*>>(visit(value));

      values.emplace_back(Statements::Inserts{
        .values = std::move(resultList),
      });
    }

    return values;
  }

  antlrcpp::Any SQLVisitorImplementation::visitSign(SQLParser::SignContext *context){
    return context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitTop(SQLParser::TopContext *context){
    return Converter<int64_t>::Stoi(context->NUMBER()->getText());
  }

  antlrcpp::Any SQLVisitorImplementation::visitDistinct(SQLParser::DistinctContext *context){
    return true;
  }

  antlrcpp::Any SQLVisitorImplementation::visitUseDbStatement(SQLParser::UseDbStatementContext *context){
    auto* statement = new Statements::UseDatabaseStatement();

    statement->name = std::any_cast<std::string>(visit(context->identifier()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateUserStatement(SQLParser::CreateUserStatementContext *context){
    auto* statement = new Statements::CreateUserStatement();

    statement->username = context->username->getText();
    statement->password = Functions::String::RemoveQuotesFromString(context->password->getText());
    statement->role = context->role->getText();

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitGrantRoleStatement(SQLParser::GrantRoleStatementContext *context){
    auto* statement = new Statements::GrantRoleStatement();

    statement->username = context->username->getText();
    statement->role = context->role->getText();

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDecimalType(SQLParser::DecimalTypeContext *context){
    return Statements::ColumnType(
      QueryPipeline::Decimal,
      Statements::DecimalType(
      Converter<int32_t>::Stoi(context->precision->getText()),
      Converter<int32_t>::Stoi(context->scale->getText())
      )
    );
  }

  antlrcpp::Any SQLVisitorImplementation::visitPrimaryKeyConstraint(SQLParser::PrimaryKeyConstraintContext *context){
    auto* statement = new Statements::PrimaryKeyConstraint();

    if (context->constraintName)
      statement->name = context->constraintName->getText();

    statement->columns = std::move(this->GetColumnsList(context->columnList()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateSchemaStatement(SQLParser::CreateSchemaStatementContext *context){

    auto* statement = new Statements::CreateSchemaStatement();

    statement->name = std::any_cast<std::string>(visit(context->identifier()));
    
    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDeleteStatement(SQLParser::DeleteStatementContext *context){
    auto* statement = new Statements::DeleteStatement();

    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    if (context->whereClause())
      statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

    return statement;
  }


  antlrcpp::Any SQLVisitorImplementation::visitColumnName(SQLParser::ColumnNameContext *context){
    Statements::ColumnName columnName;

    if (context->columnAlias())
      columnName.alias = std::any_cast<std::string>(visit(context->columnAlias()));

    if (context->identifier()) {
      columnName.name = std::any_cast<std::string>(visit(context->identifier()));
      return columnName;
    }

    if (context->MULTIPLICATION()) {
      columnName.name = context->MULTIPLICATION()->getText();
      return columnName;
    }

    throw SyntaxError("Column name was not specified", CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitTableName(SQLParser::TableNameContext *context) {

    if (!context->name)
      throw SyntaxError("No table was specified", CreatePositionErrorMessage(context));

    auto* statement = new Statements::DataSource();

    if (context->databaseName && context->schemaName) {
      statement->database = std::any_cast<std::string>(visit(context->databaseName));
      statement->schema = std::any_cast<std::string>(visit(context->schemaName));
    }
    else if (context->databaseName)
      statement->schema = std::any_cast<std::string>(visit(context->databaseName));
    else if (context->schemaName)
      statement->schema = std::any_cast<std::string>(visit(context->schemaName));

    statement->name = std::any_cast<std::string>(visit(context->name));

    if (context->alias())
      statement->alias = std::any_cast<std::string>(visit(context->alias()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlias(SQLParser::AliasContext *context) {
    return visit(context->identifier());
  }

  antlrcpp::Any SQLVisitorImplementation::visitColumnAlias(SQLParser::ColumnAliasContext *context){
    return visit(context->identifier());
  }

  antlrcpp::Any SQLVisitorImplementation::visitIdentifier(SQLParser::IdentifierContext *context){
    return context->IDENTIFIER()->getText();
  }

  std::vector<Statements::ColumnName> SQLVisitorImplementation::GetColumnsList(SQLParser::ColumnListContext *context){
    std::vector<Statements::ColumnName> columns;

    for (const auto& columnName : context->columnName())
      columns.push_back(std::move(std::any_cast<Statements::ColumnName>(visit(columnName))));

    return columns;
  }

  antlrcpp::Any SQLVisitorImplementation::visitColumnList(SQLParser::ColumnListContext *context){
      std::vector<std::string> columns;
    
      for (const auto &columnName : context->columnName())
        columns.push_back(columnName->getText());

      return columns; // Return vector of column names
  }

  antlrcpp::Any SQLVisitorImplementation::visitUpdateColumn(SQLParser::UpdateColumnContext *context){
    auto* update = new Statements::UpdateColumn();

    update->name = std::any_cast<Statements::ColumnName>(visit(context->columnName()));

    const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));

    update->value = expression;

    return update;
}
  antlrcpp::Any SQLVisitorImplementation::visitUpdateColumnsList(SQLParser::UpdateColumnsListContext *context){
    vector<Statements::UpdateColumn*> columns;

    for(const auto& updateColumn : context->updateColumn())
      columns.push_back(std::any_cast<Statements::UpdateColumn*>(visit(updateColumn)));

    return columns;
  }

  antlrcpp::Any SQLVisitorImplementation::visitUpdateStatement(SQLParser::UpdateStatementContext *context){
    auto* statement = new Statements::UpdateStatement();

    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    statement->updates = std::any_cast<std::vector<Statements::UpdateColumn*>>(visit(context->updateColumnsList()));

    if (context->whereClause() != nullptr)
      statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrderByStatement(SQLParser::OrderByStatementContext *context){
    auto* statement = new Statements::OrderByStatement();

    statement->columns = std::move(std::any_cast<std::vector<Statements::OrderColumn*>>(visit(context->orderColumnList())));

    if (statement->columns.empty())
      throw SyntaxError("No columns were specified in the order by statement", CreatePositionErrorMessage(context));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateIndexStatement(SQLParser::CreateIndexStatementContext *context){
    auto* statement = new Statements::CreateIndexStatement();

    statement->isUnique = context->UNIQUE() != nullptr;
    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    statement->name = std::any_cast<std::string>(visit(context->identifier()));

    auto colCtx = context->columnList();
    for (const auto col : colCtx->columnName())
      statement->columns.push_back(col->getText());

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableStatement(SQLParser::AlterTableStatementContext *context){
    auto* statement = new Statements::AlterTableStatement();

    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    const auto& action = context->alterTableAction();

    if (action->alterTableAddColumn()) {
      statement->newColumn = std::any_cast<Statements::NewColumn*>(visit(action->alterTableAddColumn()));
      statement->type = AlterTableType::AddColumn;
      return statement;
    }

    if (action->alterTableModifyColumn()) {
      statement->alterColumn = std::any_cast<Statements::AlterColumn*>(visit(action->alterTableModifyColumn()));
      statement->type = AlterTableType::AlterColumn;
      return statement;
    }

    if (action->alterTableDropColumn()) {
      statement->dropColumn = std::any_cast<Statements::DropColumn*>(visit(action->alterTableDropColumn()));
      statement->type = AlterTableType::DropColumn;
      return statement;
    }

    if (action->alterTableRenameColumn()) {
      statement->renameColumn = std::any_cast<Statements::RenameColumn*>(visit(action->alterTableRenameColumn()));
      statement->type = AlterTableType::RenameColumn;
      return statement;
    }

    throw SyntaxError("Unsupported action", CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableAction(SQLParser::AlterTableActionContext *context){
    return context;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableAddColumn(SQLParser::AlterTableAddColumnContext *context){
    return visit(context->addColumn());
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableDropColumn(SQLParser::AlterTableDropColumnContext *context){
    return new Statements::DropColumn{
      .name = std::any_cast<Statements::ColumnName>(visit(context->columnName()))
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableModifyColumn(SQLParser::AlterTableModifyColumnContext *context){
    return new Statements::AlterColumn{
      .name = std::any_cast<Statements::ColumnName>(visit(context->columnName())),
      .type = std::any_cast<Statements::ColumnType>(visit(context->dataType()))
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableRenameColumn(SQLParser::AlterTableRenameColumnContext *context){
    return new Statements::RenameColumn{
      .oldName = std::any_cast<Statements::ColumnName>(visit(context->oldName)),
      .newName = std::any_cast<Statements::ColumnName>(visit(context->newName)),
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitDefaultValue(SQLParser::DefaultValueContext *context){
    return visit(context->literalValue());
  }

  antlrcpp::Any SQLVisitorImplementation::visitDeclareVariableStatement(SQLParser::DeclareVariableStatementContext *context){
    auto* statement = new Statements::DeclareVariableStatement();

    if (context->resultExpression()) {
      const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
      statement->expression = expression;
    }

    statement->variable.SetValue(Value(nullptr, 0));

    auto name = std::any_cast<std::string>(visit(context->variableName()));

    statement->variable.SetName(name);

    const auto type = (context->variableType())
        ? std::any_cast<DataType>(visit(context->variableType()))
        : statement->variable.GetValue().GetType();

    statement->variable.SetType(type);

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitVariableName(SQLParser::VariableNameContext *context){
    return "@" + context->IDENTIFIER()->getText();
  }

  //optional type inference is recommended
  antlrcpp::Any SQLVisitorImplementation::visitVariableType(SQLParser::VariableTypeContext *context){
    DataType type;

    const auto text = context->getText();

    if (!ColumnTypesDictionary.TryGetValue(Functions::String::NormalizeString(text), type)) {
      throw SyntaxError("Datatype: " + text + " does not exist", CreatePositionErrorMessage(context));
    }

    return type;
  }

  antlrcpp::Any SQLVisitorImplementation::visitSetVariableStatement(SQLParser::SetVariableStatementContext *context){
    auto* statement = new Statements::SetVariableStatement();

    if (context->resultExpression()) {
      const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
      statement->expression = expression;
    }

    statement->variable.SetValue(Value(nullptr, 0));

    auto name = std::any_cast<std::string>(visit(context->variableName()));

    statement->variable.SetName(name);

    const auto type = (context->variableType())
        ? std::any_cast<DataType>(visit(context->variableType()))
        : statement->variable.GetValue().GetType();

    statement->variable.SetType(type);

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitResultList(SQLParser::ResultListContext *context){
    std::vector<Expressions::Expression*> columns;

    for (const auto& resultExpression : context->resultExpression()) {
      const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(resultExpression));

      columns.push_back(expression);
    }

    return columns;
  }

  antlrcpp::Any SQLVisitorImplementation::visitResultValue(SQLParser::ResultValueContext *context){
      if (context->columnName()) {
        const auto columnName = std::any_cast<Statements::ColumnName>(visit(context->columnName()));

        return ExpressionWrapper{ new Expressions::ColumnExpression(columnName.name, columnName.alias)};
      }

    if (context->functionCall())
      return ExpressionWrapper{ std::any_cast<Expressions::FunctionExpression*>(visit(context->functionCall())) };

    if (context->literalValue())
     return ExpressionWrapper{
        new Expressions::LiteralExpression(std::any_cast<Value>(visit(context->literalValue())))
      };

    if (context->variableName()) {
      return ExpressionWrapper{
        new Expressions::VariableExpression(std::any_cast<std::string>(visit(context->variableName())))
      };
    }

    throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));
  }



  antlrcpp::Any SQLVisitorImplementation::visitFunctionCall(SQLParser::FunctionCallContext *context){
    const auto name = std::any_cast<std::string>(visit(context->functionName()));

    Constants::FunctionType type;
    if (!Expressions::FunctionTypeDictionary.TryGetValue(Functions::String::NormalizeString(name), type))
        throw SyntaxError("Failed to parse function name: " + name, CreatePositionErrorMessage(context));

    if (!context->LAPRENT() || !context->RAPRENT())
      throw SyntaxError("Missing Closing Identetations on function: " + name, CreatePositionErrorMessage(context));

    std::vector<Expressions::Expression*> arguments;

    for (const auto& resultExpression : context->resultExpression()) {
      const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(resultExpression));

      arguments.push_back(expression);
    }

    return new Expressions::FunctionExpression(type, arguments);
  }

  antlrcpp::Any SQLVisitorImplementation::visitFunctionName(SQLParser::FunctionNameContext *context){
    if (context->IDENTIFIER())
      return context->IDENTIFIER()->getText();

    if (context->LEFT())
      return context->LEFT()->getText();

    if (context->RIGHT())
      return context->RIGHT()->getText();

    throw SyntaxError("Failed to parse function name: " + context->getText(), CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitResultExpression(SQLParser::ResultExpressionContext *context){
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->andExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->andExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->andExpr(i)));

      expression = new Expressions::LogicalExpression(expression, right, Expressions::ExpressionType::Or);
    }

    expression->name = (context->alias()) ? std::any_cast<std::string>(visit(context->alias())) : "";

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitAndExpr(SQLParser::AndExprContext *context){
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->equalityExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->equalityExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->equalityExpr(i)));

      expression = new Expressions::LogicalExpression(expression, right, Expressions::ExpressionType::And);
    }

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitEqualityExpr(SQLParser::EqualityExprContext *context){
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->relationalExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->relationalExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->relationalExpr(i)));

      const auto operation = std::any_cast<std::string>(visit(context->atomicOperator().at(i - 1)));

      Expressions::ExpressionOperator operationType;
      if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
        throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

      expression = new Expressions::BinaryExpression(expression, right, operationType);
    }

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitAtomicOperator(SQLParser::AtomicOperatorContext *context){
      return context->EQUAL()
          ? context->EQUAL()->getText()
          : context->NOTEQUAL()->getText();
    }

  antlrcpp::Any SQLVisitorImplementation::visitRelationalExpr(SQLParser::RelationalExprContext *context){
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->additiveExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->additiveExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->additiveExpr(i)));

      const auto operation = std::any_cast<std::string>(visit(context->relationalOperator().at(i - 1)));

      Expressions::ExpressionOperator operationType;
      if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
        throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

      expression = new Expressions::BinaryExpression(expression, right, operationType);
    }

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitRelationalOperator(SQLParser::RelationalOperatorContext *context){
      if (context->LESSTHAN())
        return context->LESSTHAN()->getText();

      if (context->LESS())
        return context->LESS()->getText();

      if (context->GREATERTHAN())
        return context->GREATERTHAN()->getText();

      if (context->GREATER())
        return context->GREATER()->getText();

    throw SyntaxError("", CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitAdditiveExpr(SQLParser::AdditiveExprContext *context){
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->multiplicativeExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->multiplicativeExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->multiplicativeExpr(i)));

      const auto operation = std::any_cast<std::string>(visit(context->additiveOperator().at(i - 1)));

      Expressions::ExpressionOperator operationType;
      if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
        throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

      expression = new Expressions::BinaryExpression(expression, right, operationType);
    }

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitAdditiveOperator(SQLParser::AdditiveOperatorContext *context){
    return (context->ADDITION())
         ? context->ADDITION()->getText()
         : context->SUBTRACTION()->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitMultiplicativeExpr(SQLParser::MultiplicativeExprContext *context){
    // Step 1: Get the first/left operand
    const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->primaryExpr(0)));

    auto* expression = leftExpression;

    for (int i = 1;i < context->primaryExpr().size(); i++) {
      const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->primaryExpr(i)));

      const auto operation = std::any_cast<std::string>(visit(context->multiplicativeOperator().at(i - 1)));

      Expressions::ExpressionOperator operationType;
      if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
        throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

      expression = new Expressions::BinaryExpression(expression, right, operationType);
    }

    return ExpressionWrapper{ expression };
  }

  antlrcpp::Any SQLVisitorImplementation::visitMultiplicativeOperator(SQLParser::MultiplicativeOperatorContext *context){
    if (context->MULTIPLICATION())
      return context->MULTIPLICATION()->getText();

    if (context->DIVISION())
      return context->DIVISION()->getText();

    if (context->MODULO())
      return context->MODULO()->getText();

    throw SyntaxError("", CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitPrimaryExpr(SQLParser::PrimaryExprContext *context){
    return context->resultExpression()
        ? std::any_cast<ExpressionWrapper>(visit(context->resultExpression()))
        : std::any_cast<ExpressionWrapper>(visit(context->resultValue()));
  }
}
