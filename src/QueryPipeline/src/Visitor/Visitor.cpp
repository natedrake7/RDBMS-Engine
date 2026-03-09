#include "../../include/Visitor.h"
#include "../../../Systemic/include/Converter.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../include/ErrorListener.h"
#include "../../include/Statements.h"
#include "../../include/CompileContext.h"
#include <vector>

#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"

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

    return std::any(statement);
  }

  antlrcpp::Any SQLVisitorImplementation::visitDropDbStatement(SQLParser::DropDbStatementContext *context) {
    auto* statement = new Statements::DropDbStatement();

    if (context->identifier())
      statement->name = std::any_cast<std::string>(visit(context->identifier()));

    return std::any(statement);
  }

    antlrcpp::Any SQLVisitorImplementation::visitSelectStatement(SQLParser::SelectStatementContext *context) {
        auto* statement = this->_compileContext->Allocate<Statements::SelectStatement>();

        if (!context->resultList())
            throw SyntaxError("No arguments specified", CreatePositionErrorMessage(context));

        if (context->top())
            statement->top = std::any_cast<BigInt>(visit(context->top()));

        statement->distinct = context->distinct() != nullptr;

        statement->results = std::any_cast<std::vector<Expressions::Expression*>>(visitResultList(context->resultList()));

        statement->table = (context->datasource() != nullptr)
            ? std::any_cast<Statements::DataSource*>(visit(context->datasource()))
            : nullptr;

        for (auto* join : context->joinStatement())
            statement->joins.push_back(std::any_cast<Statements::JoinStatement*>(visit(join)));

        if (context->whereClause() != nullptr)
            statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

        if(context->orderByStatement())
            statement->orderBy = std::any_cast<Statements::OrderByStatement*>(visit(context->orderByStatement()));

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitDatasource(SQLParser::DatasourceContext *context) {
        if (context->tableName())
            return visit(context->tableName());

        if (context->selectStatement())
            return visit(context->selectStatement());

        throw SyntaxError("Invalid Data source specified", CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitBranchingExpression(SQLParser::BranchingExpressionContext *context) {
        if (context->switchExpression())
            return this->visit(context->switchExpression());
        return this->visit(context->ternaryExpression());
    }

    antlrcpp::Any SQLVisitorImplementation::visitSwitchExpression(SQLParser::SwitchExpressionContext *context) {
        auto* expression = this->_compileContext->Allocate<Expressions::BranchExpression>(Expressions::BranchType::Switch);

        for (const auto& caseExpression : context->caseExpression()) {
            const auto& [branch] = std::any_cast<ExpressionWrapper>(visit(caseExpression->branch));
            expression->branches.push_back(branch);

            const auto& [result] = std::any_cast<ExpressionWrapper>(visit(caseExpression->result));
            expression->results.push_back(result);
        }

        const auto& [baseCase] = std::any_cast<ExpressionWrapper>(visit(context->baseCase));
        expression->baseCase = baseCase;

        return std::any(expression);
    }

    antlrcpp::Any SQLVisitorImplementation::visitCaseExpression(SQLParser::CaseExpressionContext *context) {

    }

    antlrcpp::Any SQLVisitorImplementation::visitTernaryExpression(SQLParser::TernaryExpressionContext *context){
        auto* expression = this->_compileContext->Allocate<Expressions::BranchExpression>(Expressions::BranchType::Ternary);

        const auto& [branch] = std::any_cast<ExpressionWrapper>(visit(context->branch));
        expression->branches.push_back(branch);

        const auto& [trueResult] = std::any_cast<ExpressionWrapper>(visit(context->trueResult));
        expression->results.push_back(trueResult);

        const auto& [falseResult] = std::any_cast<ExpressionWrapper>(visit(context->falseResult));
        expression->results.push_back(falseResult);

        return std::any(expression);
    }

    antlrcpp::Any SQLVisitorImplementation::visitWhereClause(SQLParser::WhereClauseContext *context){
        Statements::WhereClause where;
        const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));

        where.expression = expression;

        return std::any(where);
    }

    antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
        if (context->STRING()) {
            const auto& str = context->STRING()->getText();
            auto value = Value(Functions::String::RemoveQuotesFromString(str), this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        if (context->UNICODESTRING()) {
            const auto& str = context->UNICODESTRING()->getText();
            auto value = Value(Functions::String::RemoveQuotesFromString(str), this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        if (context->NUMBER()) {
            const auto& numberStr = (context->sign())
                ? context->sign()->getText() + context->NUMBER()->getText()
                : context->NUMBER()->getText();

            const auto number = Converter<BigInt>::Stoi(numberStr);
            auto value = Value(number, this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        if (context->DECIMAL_REGEX()) {
            const auto& decimalStr = (context->sign())
                ? context->sign()->getText() + context->DECIMAL_REGEX()->getText()
                : context->DECIMAL_REGEX()->getText();

            const auto view = DataTypes::StringView(decimalStr.c_str(), decimalStr.size());
            const auto decimal = DataTypes::Decimal(view);
            auto value = Value(decimal, this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        if (context->NULL_()){
            auto value = Value::Null();
            return std::any(value);
        }

        if (context->TRUE()){
            auto value = Value(true, this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        if (context->FALSE()){
            auto value = Value(false, this->_compileContext->GetAllocator(), 0);
            return std::any(value);
        }

        throw SyntaxError("Invalid value specified" + context->getText(), CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitInsertStatement(SQLParser::InsertStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::InsertStatement>();

        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));
        statement->columns = std::move(this->GetColumnsList(context->columnList()));

        if (context->valuesStatement())
            statement->values = std::any_cast<std::vector<Statements::Inserts>>(visit(context->valuesStatement()));

        if (context->selectStatement())
            statement->selectStatement = std::any_cast<Statements::SelectStatement*>(visit(context->selectStatement()));

        if (context->valuesStatement() && context->selectStatement())
            throw SyntaxError("Cannot have both Values Statement and Select Statement in insert", CreatePositionErrorMessage(context));

        if (!context->selectStatement() && !context->valuesStatement())
            throw SyntaxError("Invalid syntax at insert", CreatePositionErrorMessage(context));

        return std::any(statement);
    }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValueList(SQLParser::LiteralValueListContext *context){
    std::vector<Value> values;
    
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
        auto columnType = Statements::ColumnType(Functions::String::NormalizeString(text));
        return std::any(columnType);
    }

    antlrcpp::Any SQLVisitorImplementation::visitStringType(SQLParser::StringTypeContext *context){
        const auto& number = context->NUMBER();

        auto typeName = std::string(QueryPipeline::String);
        auto column = Statements::ColumnType(
            typeName,
            number
                ? Converter<Int>::Stoi(number->getText())
                : -1
        );
        return std::any(column);
    }

    antlrcpp::Any SQLVisitorImplementation::visitUStringType(SQLParser::UStringTypeContext *context){
        const auto& number = context->NUMBER();

        auto typeName = std::string(QueryPipeline::UnicodeString);
        auto column = Statements::ColumnType(
            typeName,
            number
                ? Converter<Int>::Stoi(number->getText())
                : -1
        );
        return std::any(column);
    }

    antlrcpp::Any SQLVisitorImplementation::visitOrderColumnList(SQLParser::OrderColumnListContext *context){
        std::vector<Statements::OrderColumn*> orderColumns;

        for (const auto& orderColumn : context->orderColumn())
            orderColumns.push_back(std::any_cast<Statements::OrderColumn*>(visit(orderColumn)));

        return std::any(orderColumns);
    }

    antlrcpp::Any SQLVisitorImplementation::visitOrderColumn(SQLParser::OrderColumnContext *context){
        auto* orderColumn = this->_compileContext->Allocate<Statements::OrderColumn>();

        const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));

        orderColumn->expression = expression;

        if (!context->order())
            return std::any(orderColumn);

        const auto orderStr = std::any_cast<std::string>(visit(context->order()));

        //TODO add to a static stringview
        orderColumn->type = Functions::String::NormalizeString(orderStr) == "desc"
            ? OrderType::DESCENDING
            : OrderType::ASCENDING;

        return std::any(orderColumn);
    }

    antlrcpp::Any SQLVisitorImplementation::visitOrder(SQLParser::OrderContext *context){
        auto value = context->ASC()
            ? context->ASC()->getText()
            : context->DESC()->getText();
        return std::any(value);
    }

    std::any SQLVisitorImplementation::visitValuesStatement(SQLParser::ValuesStatementContext *context){
        return visit(context->valuesList());
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

        return std::any(values);
    }

    antlrcpp::Any SQLVisitorImplementation::visitSign(SQLParser::SignContext *context){
        auto value = context->getText();
        return std::any(value);
    }

    antlrcpp::Any SQLVisitorImplementation::visitTop(SQLParser::TopContext *context){
        auto value = Converter<int64_t>::Stoi(context->NUMBER()->getText());
        return std::any(value);
    }

    antlrcpp::Any SQLVisitorImplementation::visitDistinct(SQLParser::DistinctContext *context){
        return std::any(true);
    }

    antlrcpp::Any SQLVisitorImplementation::visitUseDbStatement(SQLParser::UseDbStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::UseDatabaseStatement>();
        statement->name = std::any_cast<std::string>(visit(context->identifier()));
        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitCreateUserStatement(SQLParser::CreateUserStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::CreateUserStatement>();

        statement->username = context->username->getText();
        statement->password = Functions::String::RemoveQuotesFromString(context->password->getText());
        statement->role = context->role->getText();

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitGrantRoleStatement(SQLParser::GrantRoleStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::GrantRoleStatement>();

        statement->username = context->username->getText();
        statement->role = context->role->getText();

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitDecimalType(SQLParser::DecimalTypeContext *context){
        auto typeName = std::string(QueryPipeline::Decimal);
        auto columnType = Statements::ColumnType(
            typeName,
            Statements::DecimalType(
            Converter<int32_t>::Stoi(context->precision->getText()),
            Converter<int32_t>::Stoi(context->scale->getText())
            )
        );

        return std::any(columnType);
    }

    antlrcpp::Any SQLVisitorImplementation::visitPrimaryKeyConstraint(SQLParser::PrimaryKeyConstraintContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::PrimaryKeyConstraint>();

        if (context->constraintName)
            statement->name = context->constraintName->getText();

        statement->columns = std::move(this->GetColumnsList(context->columnList()));
        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitCreateSchemaStatement(SQLParser::CreateSchemaStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::CreateSchemaStatement>();
        statement->name = std::any_cast<std::string>(visit(context->identifier()));
        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitDeleteStatement(SQLParser::DeleteStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::DeleteStatement>();

        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

        if (context->whereClause())
            statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

        return std::any(statement);
    }


    antlrcpp::Any SQLVisitorImplementation::visitColumnName(SQLParser::ColumnNameContext *context){
        Statements::ColumnName columnName;

        if (context->columnAlias())
            columnName.alias = std::any_cast<DataTypes::String>(visit(context->columnAlias()));

        if (context->identifier()) {
            columnName.name = std::any_cast<DataTypes::String>(visit(context->identifier()));
            return std::any(columnName);
        }

        if (context->MULTIPLICATION()) {
            const auto value = context->MULTIPLICATION()->getText();
            columnName.name = DataTypes::String(value, this->_compileContext->GetAllocator());
            return std::any(columnName);
        }

        throw SyntaxError("Column name was not specified", CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitTableName(SQLParser::TableNameContext *context) {
        if (!context->name)
            throw SyntaxError("No table was specified", CreatePositionErrorMessage(context));

        auto* statement = this->_compileContext->Allocate<Statements::DataSource>();

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
        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlias(SQLParser::AliasContext *context) {
        return visit(context->identifier());
    }

    antlrcpp::Any SQLVisitorImplementation::visitColumnAlias(SQLParser::ColumnAliasContext *context){
        return visit(context->identifier());
    }

    antlrcpp::Any SQLVisitorImplementation::visitIdentifier(SQLParser::IdentifierContext *context){
        const auto value = context->IDENTIFIER()->getText();
        auto castValue = DataTypes::String(value, this->_compileContext->GetAllocator());
        return std::any(castValue);
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

        return std::any(columns); // Return vector of column names
    }

    antlrcpp::Any SQLVisitorImplementation::visitUpdateColumn(SQLParser::UpdateColumnContext *context){
        auto* update = this->_compileContext->Allocate<Statements::UpdateColumn>();
        update->name = std::any_cast<Statements::ColumnName>(visit(context->columnName()));

        const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
        update->value = expression;

        return std::any(update);
    }

    antlrcpp::Any SQLVisitorImplementation::visitUpdateColumnsList(SQLParser::UpdateColumnsListContext *context){
        std::vector<Statements::UpdateColumn*> columns;

        for(const auto& updateColumn : context->updateColumn())
            columns.push_back(std::any_cast<Statements::UpdateColumn*>(visit(updateColumn)));

        return std::any(columns);
    }

    antlrcpp::Any SQLVisitorImplementation::visitUpdateStatement(SQLParser::UpdateStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::UpdateStatement>();

        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));
        statement->updates = std::any_cast<std::vector<Statements::UpdateColumn*>>(visit(context->updateColumnsList()));

        if (context->whereClause() != nullptr)
            statement->where = std::any_cast<Statements::WhereClause>(visit(context->whereClause()));

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitOrderByStatement(SQLParser::OrderByStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::OrderByStatement>();

        statement->columns = std::move(std::any_cast<std::vector<Statements::OrderColumn*>>(visit(context->orderColumnList())));

        if (statement->columns.empty())
            throw SyntaxError("No columns were specified in the order by statement", CreatePositionErrorMessage(context));

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitCreateIndexStatement(SQLParser::CreateIndexStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::CreateIndexStatement>();

        statement->isUnique = context->UNIQUE() != nullptr;
        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

        statement->name = std::any_cast<std::string>(visit(context->identifier()));

        const auto colCtx = context->columnList();
        for (const auto col : colCtx->columnName())
            statement->columns.push_back(col->getText());

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableStatement(SQLParser::AlterTableStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::AlterTableStatement>();

        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

        const auto& action = context->alterTableAction();

        if (action->alterTableAddColumn()) {
            statement->column.newColumn = std::any_cast<Statements::NewColumn*>(visit(action->alterTableAddColumn()));
            statement->type = AlterTableType::AddColumn;
            return std::any(statement);
        }

        if (action->alterTableModifyColumn()) {
            statement->column.alterColumn = std::any_cast<Statements::AlterColumn*>(visit(action->alterTableModifyColumn()));
            statement->type = AlterTableType::AlterColumn;
            return std::any(statement);
        }

        if (action->alterTableDropColumn()) {
            statement->column.dropColumn = std::any_cast<Statements::DropColumn*>(visit(action->alterTableDropColumn()));
            statement->type = AlterTableType::DropColumn;
            return std::any(statement);
        }

        if (action->alterTableRenameColumn()) {
            statement->column.renameColumn = std::any_cast<Statements::RenameColumn*>(visit(action->alterTableRenameColumn()));
            statement->type = AlterTableType::RenameColumn;
            return std::any(statement);
        }

        throw SyntaxError("Unsupported action", CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableAction(SQLParser::AlterTableActionContext *context){
        return std::any(context);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableAddColumn(SQLParser::AlterTableAddColumnContext *context){
        return visit(context->addColumn());
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableDropColumn(SQLParser::AlterTableDropColumnContext *context){
        auto* dropColumn = this->_compileContext->Allocate<Statements::DropColumn>();
        dropColumn->name = std::any_cast<Statements::ColumnName>(visit(context->columnName()));
        return std::any(dropColumn);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableModifyColumn(SQLParser::AlterTableModifyColumnContext *context){
        auto* alterColumn = this->_compileContext->Allocate<Statements::AlterColumn>();
        alterColumn->name = std::any_cast<Statements::ColumnName>(visit(context->columnName()));
        alterColumn->type = std::any_cast<Statements::ColumnType>(visit(context->dataType()));
        return std::any(alterColumn);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAlterTableRenameColumn(SQLParser::AlterTableRenameColumnContext *context){
        auto* renameColumn = this->_compileContext->Allocate<Statements::RenameColumn>();
        renameColumn->oldName = std::any_cast<Statements::ColumnName>(visit(context->oldName));
        renameColumn->newName = std::any_cast<Statements::ColumnName>(visit(context->newName));
        return std::any(renameColumn);
    }

    antlrcpp::Any SQLVisitorImplementation::visitDefaultValue(SQLParser::DefaultValueContext *context){
        return visit(context->literalValue());
    }

    antlrcpp::Any SQLVisitorImplementation::visitDeclareVariableStatement(SQLParser::DeclareVariableStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::DeclareVariableStatement>();

        if (context->resultExpression()) {
            const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
            statement->expression = expression;
        }

        statement->variable.SetValue(Value::Null());

        auto name = std::any_cast<std::string>(visit(context->variableName()));

        statement->variable.SetName(name);

        const auto type = (context->variableType())
            ? std::any_cast<DataType>(visit(context->variableType()))
            : statement->variable.GetValue().GetType();

        statement->variable.SetType(type);
        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitVariableName(SQLParser::VariableNameContext *context){
        auto value = "@" + context->IDENTIFIER()->getText();
        return std::any(value);
    }

    //optional type inference is recommended
    antlrcpp::Any SQLVisitorImplementation::visitVariableType(SQLParser::VariableTypeContext *context){
        DataType type;

        const auto text = context->getText();
        const auto normalizedText = Functions::String::NormalizeString(text);
        const auto view = DataTypes::StringView(normalizedText.c_str(), normalizedText.size());

        if (!ColumnTypesDictionary.TryGetValue(view, type))
            throw SyntaxError("Datatype: " + text + " does not exist", CreatePositionErrorMessage(context));

        return std::any(type);
    }

    antlrcpp::Any SQLVisitorImplementation::visitSetVariableStatement(SQLParser::SetVariableStatementContext *context){
        auto* statement = this->_compileContext->Allocate<Statements::SetVariableStatement>();

        if (context->resultExpression()) {
            const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
            statement->expression = expression;
        }

        statement->variable.SetValue(Value::Null());

        auto name = std::any_cast<std::string>(visit(context->variableName()));

        statement->variable.SetName(name);

        const auto type = (context->variableType())
            ? std::any_cast<DataType>(visit(context->variableType()))
            : statement->variable.GetValue().GetType();

        statement->variable.SetType(type);

        return std::any(statement);
    }

    antlrcpp::Any SQLVisitorImplementation::visitResultList(SQLParser::ResultListContext *context){
        std::vector<Expressions::Expression*> columns;

        for (const auto& resultExpression : context->resultExpression()) {
            const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(resultExpression));
            columns.push_back(expression);
        }

        return std::any(columns);
    }

    antlrcpp::Any SQLVisitorImplementation::visitResultValue(SQLParser::ResultValueContext *context){
        auto wrapper = ExpressionWrapper();

        if (context->columnName()) {
            const auto columnName = std::any_cast<Statements::ColumnName>(visit(context->columnName()));
            wrapper.expression = this->_compileContext->Allocate<Expressions::ColumnExpression>(columnName.name, columnName.alias);
            return std::any(wrapper);
        }

        if (context->functionCall()){
            wrapper.expression = std::any_cast<Expressions::FunctionExpression*>(visit(context->functionCall()));
            return std::any(wrapper);
        }

        if (context->literalValue()){
            wrapper.expression = this->_compileContext->Allocate<Expressions::ConstantExpression>(
                std::any_cast<Value>(visit(context->literalValue()))
            );
            return std::any(wrapper);
        }

        if (context->variableName())
            wrapper.expression = this->_compileContext->Allocate<Expressions::VariableExpression>(
                std::any_cast<DataTypes::String>(visit(context->variableName())),
                this->_compileContext->GetAllocator()
            );
            return std::any(wrapper);

        if (context->branchingExpression()){
            wrapper.expression = std::any_cast<Expressions::BranchExpression*>(visit(context->branchingExpression()));
            return std::any(wrapper);
        }

        throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitFunctionCall(SQLParser::FunctionCallContext *context){
        const auto name = std::any_cast<std::string>(visit(context->functionName()));

        FunctionType type;
        if (!Expressions::FunctionTypeDictionary.TryGetValue(Functions::String::NormalizeString(name), type))
            throw SyntaxError("Failed to parse function name: " + name, CreatePositionErrorMessage(context));

        if (!context->LAPRENT() || !context->RAPRENT())
            throw SyntaxError("Missing Closing Indentations on function: " + name, CreatePositionErrorMessage(context));

        std::vector<Expressions::Expression*> arguments;

        for (const auto& resultExpression : context->resultExpression()) {
            const auto& [expression] = std::any_cast<ExpressionWrapper>(visit(resultExpression));
            arguments.push_back(expression);
        }

        auto* expression = this->_compileContext->Allocate<Expressions::FunctionExpression>(type, arguments);
        return std::any(expression);
    }

    antlrcpp::Any SQLVisitorImplementation::visitFunctionName(SQLParser::FunctionNameContext *context){
        std::string value;
        if (context->IDENTIFIER()){
            value = context->IDENTIFIER()->getText();
            return std::any(value);
        }

        if (context->LEFT()){
            value = context->LEFT()->getText();
            return std::any(value);
        }

        if (context->RIGHT()){
            value = context->RIGHT()->getText();
            return std::any(value);
        }

        throw SyntaxError("Failed to parse function name: " + context->getText(), CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitResultExpression(SQLParser::ResultExpressionContext *context){
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->andExpr(0)));

        auto* expression = leftExpression;
        for (int i = 1;i < context->andExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->andExpr(i)));
            expression = this->_compileContext->Allocate<Expressions::LogicalExpression>(expression, right, Expressions::LogicalType::Or);
        }

        if (context->alias()){
            auto value = std::any_cast<DataTypes::String>(visit(context->alias()));
            expression->name = std::move(value);
        }
        else
          expression->name = DataTypes::String::Empty(this->_compileContext->GetAllocator());

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAndExpr(SQLParser::AndExprContext *context){
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->equalityExpr(0)));

        auto* expression = leftExpression;

        for (int i = 1;i < context->equalityExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->equalityExpr(i)));
            expression = this->_compileContext->Allocate<Expressions::LogicalExpression>(expression, right, Expressions::LogicalType::And);
        }

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitEqualityExpr(SQLParser::EqualityExprContext *context){
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->relationalExpr(0)));
        auto* expression = leftExpression;

        for (int i = 1;i < context->relationalExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->relationalExpr(i)));

            const auto operation = std::any_cast<std::string>(visit(context->atomicOperator().at(i - 1)));

            Expressions::BinaryOperator operationType;
            if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
                throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

            expression = this->_compileContext->Allocate<Expressions::BinaryExpression>(expression, right, operationType);
        }

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAtomicOperator(SQLParser::AtomicOperatorContext *context){
        auto value = context->EQUAL()
            ? context->EQUAL()->getText()
            : context->NOTEQUAL()->getText();
        return std::any(value);
    }

    antlrcpp::Any SQLVisitorImplementation::visitRelationalExpr(SQLParser::RelationalExprContext *context){
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->additiveExpr(0)));
        auto* expression = leftExpression;

        for (int i = 1;i < context->additiveExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->additiveExpr(i)));
            const auto operation = std::any_cast<std::string>(visit(context->relationalOperator().at(i - 1)));

            Expressions::BinaryOperator operationType;
            if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
                throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

            expression = this->_compileContext->Allocate<Expressions::BinaryExpression>(expression, right, operationType);
        }

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitRelationalOperator(SQLParser::RelationalOperatorContext *context){
        std::string value;
        if (context->LESSTHAN()){
            value = context->LESSTHAN()->getText();
            return std::any(value);
        }
        if (context->LESS()){
            value = context->LESS()->getText();
            return std::any(value);
        }
        if (context->GREATERTHAN()){
            value = context->GREATERTHAN()->getText();
            return std::any(value);
        }
        if (context->GREATER()){
            value = context->GREATER()->getText();
            return std::any(value);
        }

        throw SyntaxError("", CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitAdditiveExpr(SQLParser::AdditiveExprContext *context){
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->multiplicativeExpr(0)));
        auto* expression = leftExpression;

        for (int i = 1;i < context->multiplicativeExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->multiplicativeExpr(i)));

            const auto operation = std::any_cast<std::string>(visit(context->additiveOperator().at(i - 1)));

            Expressions::BinaryOperator operationType;
            if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
                throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

            expression = this->_compileContext->Allocate<Expressions::BinaryExpression>(expression, right, operationType);
        }

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitAdditiveOperator(SQLParser::AdditiveOperatorContext *context){
        auto value = (context->ADDITION())
            ? context->ADDITION()->getText()
            : context->SUBTRACTION()->getText();

        return std::any(value);
    }

    antlrcpp::Any SQLVisitorImplementation::visitMultiplicativeExpr(SQLParser::MultiplicativeExprContext *context){
        // Step 1: Get the first/left operand
        const auto& [leftExpression] = std::any_cast<ExpressionWrapper>(visit(context->primaryExpr(0)));
        auto* expression = leftExpression;

        for (int i = 1;i < context->primaryExpr().size(); i++) {
            const auto& [right] = std::any_cast<ExpressionWrapper>(visit(context->primaryExpr(i)));
            const auto operation = std::any_cast<std::string>(visit(context->multiplicativeOperator().at(i - 1)));

            Expressions::BinaryOperator operationType;
            if (!Expressions::ExpressionOperatorsDictionary.TryGetValue(operation, operationType))
                throw SyntaxError("Invalid Operation Type specified: " + operation, CreatePositionErrorMessage(context));

            expression = this->_compileContext->Allocate<Expressions::BinaryExpression>(expression, right, operationType);
        }

        auto wrapper = ExpressionWrapper{ expression };
        return std::any(wrapper);
    }

    antlrcpp::Any SQLVisitorImplementation::visitMultiplicativeOperator(SQLParser::MultiplicativeOperatorContext *context){
        std::string value;
        if (context->MULTIPLICATION()){
            value = context->MULTIPLICATION()->getText();
            return std::any(value);
        }
        if (context->DIVISION()){
            value = context->DIVISION()->getText();
            return std::any(value);
        }
        if (context->MODULO()){
            value = context->MODULO()->getText();
            return std::any(value);
        }

        throw SyntaxError("", CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitPrimaryExpr(SQLParser::PrimaryExprContext *context){
        return context->resultExpression()
            ? visit(context->resultExpression())
            : visit(context->resultValue());
    }
}
