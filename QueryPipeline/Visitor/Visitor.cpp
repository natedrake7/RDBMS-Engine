#include "Visitor.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../ErrorListener/ErrorListener.h"
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

antlrcpp::Any SQLVisitorImplementation::visitSelectStatement(SQLParser::SelectStatementContext *ctx) {
    auto* statement = new Statements::SelectStatement();

    if (!ctx->resultList())
      throw SyntaxError("No arguments specified", CreatePositionErrorMessage(ctx));

    statement->results = std::any_cast<std::vector<Expressions::Expression*>>(visitResultList(ctx->resultList()));

    statement->table = (ctx->tableName() != nullptr)
              ? std::any_cast<Statements::TableName*>(visit(ctx->tableName()))
              : nullptr;

    for (const auto join : ctx->joinStatement())
      statement->joins.push_back(std::any_cast<Statements::JoinStatement*>(visit(join)));

    if (ctx->whereClause() != nullptr)
      statement->where = std::any_cast<Statements::WhereClause>(visit(ctx->whereClause()));

    if(ctx->orderByStatement())
      statement->orderBy = std::any_cast<Statements::OrderByStatement*>(visit(ctx->orderByStatement()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitWhereClause(SQLParser::WhereClauseContext *context){
    Statements::WhereClause where;
    where.expression = std::any_cast<Expressions::LogicalExpression*>(visitExpression(context->expression()));
    return where;
  }

  antlrcpp::Any SQLVisitorImplementation::visitExpression(SQLParser::ExpressionContext *context){
    return visit(context->orExpression());
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
    if (context->STRING()) {
      const auto& str = context->STRING()->getText();

      return Field(AdditionalLibraries::StringFunctions::RemoveQuotesFromString(str), 0);
    }

    if (context->UNICODESTRING()) {
      const auto& str = context->UNICODESTRING()->getText();

      const auto parsedStr = AdditionalLibraries::StringFunctions::RemoveQuotesFromUnicodeString(str);

      return Field(AdditionalLibraries::StringFunctions::ToUnicode(parsedStr), 0);
    }

    if (context->NUMBER()) {
      const auto number = SafeConverter<int64_t>::SafeStoi(context->NUMBER()->getText());

      return Field(number, 0);
    }

    if (context->NULL_())
      return Field(nullptr, 0);

    if (context->TRUE())
      return Field(true, 0);

    if (context->FALSE())
      return Field(false, 0);

    throw SyntaxError("Invalid value specified" + context->getText(), CreatePositionErrorMessage(context));
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrExpression(SQLParser::OrExpressionContext *context){
    auto* expression = std::any_cast<Expressions::LogicalExpression*>(visit(context->andExpression(0)));

    for (size_t i = 1; i < context->andExpression().size(); i++) {
      auto* right = std::any_cast<Expressions::LogicalExpression*>(visit(context->andExpression(i)));

      expression = Expressions::LogicalExpression::Logical(Expressions::ExpressionType::Or, expression, right);
    }

    return expression;
  }

antlrcpp::Any SQLVisitorImplementation::visitAndExpression(SQLParser::AndExpressionContext *context) {
    auto* expression = std::any_cast<Expressions::LogicalExpression*>(visit(context->predicate(0)));

    for (size_t i = 1; i < context->predicate().size(); i++) {
      auto* right = std::any_cast<Expressions::LogicalExpression*>(visit(context->predicate(i)));

      expression = Expressions::LogicalExpression::Logical(Expressions::ExpressionType::And, expression, right);
    }

    return expression;
  }
  antlrcpp::Any SQLVisitorImplementation::visitPredicate(SQLParser::PredicateContext *context){
    if (context->expression())
      return visit(context->expression());

    if (context->op == nullptr)
      throw invalid_argument("Invalid operation specified");

    Expressions::ExpressionOperator expressionOperator;
    if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(context->op->getText(), expressionOperator))
        throw invalid_argument("Invalid operation specified");

    const auto columnName = std::any_cast<Statements::ColumnName>(visit(context->columnName()));

    return  Expressions::LogicalExpression::Predicate(
      columnName.alias,
      columnName.name,
      expressionOperator,
      std::any_cast<Field>(visit(context->literalValue()))
    );
  }

  antlrcpp::Any SQLVisitorImplementation::visitInsertStatement(SQLParser::InsertStatementContext *context){
    auto* statement = new Statements::InsertStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    statement->columns = std::move(this->GetColumnsList(context->columnList()));
    
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


antlrcpp::Any SQLVisitorImplementation::visitDataType(SQLParser::DataTypeContext *context) {
    if (context->varcharType())
      return visit(context->varcharType());
    if (context->nvarcharType())
      return visit(context->varcharType());

    const auto& text = context->getText();

    return Statements::ColumnType{
      .name = AdditionalLibraries::StringFunctions::NormalizeString(text),
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitVarcharType(SQLParser::VarcharTypeContext *context){
    const auto& number = context->NUMBER();

    //TODO handle VARCHAR(MAX) types
    if(!number)
    {

    }

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

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

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

    auto* statement = new Statements::TableName();

    if (context->schemaName)
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
    return Statements::UpdateColumnStatement{
      .name = std::any_cast<Statements::ColumnName>(visit(context->columnName())),
      .value = std::any_cast<Field>(visit(context->literalValue()))
    };
}
  antlrcpp::Any SQLVisitorImplementation::visitUpdateColumnsList(SQLParser::UpdateColumnsListContext *context){
    vector<Statements::UpdateColumnStatement> columns;

    for(const auto& updateColumn : context->updateColumn())
      columns.push_back(std::any_cast<Statements::UpdateColumnStatement>(visit(updateColumn)));

    return columns;
  }

  antlrcpp::Any SQLVisitorImplementation::visitUpdateStatement(SQLParser::UpdateStatementContext *context){
    auto* statement = new Statements::UpdateStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    statement->columns = std::any_cast<std::vector<Statements::UpdateColumnStatement>>(visit(context->updateColumnsList()));

    if (context->whereClause() != nullptr)
      statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrderByStatement(SQLParser::OrderByStatementContext *context){
    auto* statement = new Statements::OrderByStatement();

    statement->columns = std::move(this->GetColumnsList(context->columnList()));
    statement->order = context->order ? context->order->getText() : "ASC";

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateIndexStatement(SQLParser::CreateIndexStatementContext *context){
    auto* statement = new Statements::CreateIndexStatement();

    statement->isUnique = context->UNIQUE() != nullptr;
    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    statement->name = std::any_cast<std::string>(visit(context->identifier()));

    auto colCtx = context->columnList();
    for (const auto col : colCtx->columnName())
      statement->columns.push_back(col->getText());

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitAlterTableStatement(SQLParser::AlterTableStatementContext *context){
    auto* statement = new Statements::AlterTableStatement();

    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));

    const auto& action = context->alterTableAction();

    if (action->alterTableAddColumn()) {
      statement->addColumn = std::any_cast<Statements::AddColumn*>(visit(action->alterTableAddColumn()));
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
    auto variableName = std::any_cast<std::string>(visit(context->variableName()));

    auto value = context->literalValue()
        ? std::any_cast<Field>(visit(context->literalValue()))
        : Field(nullptr, 0);

    value.SetName(variableName);

    value.InferType();

    std::cout << "Variable declared: " << variableName << " with value: " << value << std::endl;

    return value;
  }

  antlrcpp::Any SQLVisitorImplementation::visitVariableName(SQLParser::VariableNameContext *context){
    return context->IDENTIFIER()->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitVariableType(SQLParser::VariableTypeContext *context){
    //optional type inference is recommended
  }

  antlrcpp::Any SQLVisitorImplementation::visitSetVariableStatement(SQLParser::SetVariableStatementContext *context){
    auto variableName = std::any_cast<std::string>(visit(context->variableName()));

    auto value = context->literalValue()
        ? std::any_cast<Field>(visit(context->literalValue()))
        : Field(nullptr, 0);

    value.SetName(variableName);

    value.InferType();

    std::cout << "Variable set: " << variableName << " with value: " << value << std::endl;

    return value;
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
      return ExpressionWrapper{new Expressions::LiteralExpression(std::any_cast<Field>(visit(context->literalValue())))};

    if (context->variableName()) {

    }

    throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));
  }



  antlrcpp::Any SQLVisitorImplementation::visitFunctionCall(SQLParser::FunctionCallContext *context){
    const auto name = std::any_cast<std::string>(visit(context->functionName()));

    Constants::FunctionType type;
    if (!Expressions::FunctionTypeDictionary.TryGetValue(AdditionalLibraries::StringFunctions::NormalizeString(name), type))
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
