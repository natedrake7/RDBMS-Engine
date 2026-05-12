#include "../include/Statements.h"

#include "../../CoreEngine/include/Database.h"
#include "../../Systemic/include/Coercions.h"
#include "../../Systemic/include/Functions/StringFunctions.h"
#include "../../Server/include/Server.h"
#include "../include/LogicalPlan.h"
#include "../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include <ranges>
#include <ValidationMessages.h>

#include "Optimizer.h"
#include "Parser.h"
#include "../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace QueryPipeline::Statements {
    Statement::Statement(){
        this->databaseId = INVALID_DATABASE_ID;
        this->table = nullptr;
        this->server = &Network::Server::Get();
        this->catalog = &CoreEngine::SystemCatalog::Get();
    }

    Errors::ValidationStatus Statement::CompileBase(const QueryContext& context)const{
        const auto* session = this->server->GetSession(this->sessionId);

        Errors::ValidationStatus validationStatus(context.GetAllocator());

        if (!session || !session->user || !session->user->role){
            validationStatus.code = Errors::ValidationError::Error;
            validationStatus.message = DataTypes::String::FromView(
                Messages::FAILED_TO_FETCH_USER_SESSION,
                context.GetAllocator()
            );
        }

        if (!session->user->role->HasPermission(this->RequiredPermissions())) {
            validationStatus.code = Errors::ValidationError::Error;
            validationStatus.message = DataTypes::String::Concat(context.GetAllocator(), "User", session->user->name, " is not authorized to perform this action.");
        }

        return validationStatus;
    }

  Errors::ValidationStatus Statement::Compile(QueryContext& context){
    auto result = this->CompileBase(context);

    if (!result.IsOk())
      return result;

    return this->CompileDerived(context);
  }

   DeclareVariableStatement::DeclareVariableStatement() {
     this->expression = nullptr;
   }

    Errors::ValidationStatus DeclareVariableStatement::CompileDerived(QueryContext& context) {
        const auto& type = this->variable.GetType();

        auto validationStatus = Errors::ValidationStatus(context.GetAllocator());
        if (this->expression) {
            auto res = CompileExpression(context, this->expression);

            if (!res.IsOk()) return res;

            if (type != DataType::Null && !ValidateExpressionCoercionTypes(type, this->expression)) {
                validationStatus.code = Errors::ValidationError::Error;
                validationStatus.message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
            Expressions::GetExpressionReturnType(this->expression),
            type
                );

                return validationStatus;
            }

            if (type == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        context._scope.variables.ForceAdd(this->variable.GetNormalizedName(), type);
        return validationStatus;
    }

    constexpr Security::Permission DeclareVariableStatement::RequiredPermissions()const {
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan * DeclareVariableStatement::ToLogical(QueryContext& context) {
        return context._compileContext.Allocate<LogicalDeclareVariable>(this->sessionId, this->variable, this->expression);
    }

    SetVariableStatement::SetVariableStatement() {
        this->expression = nullptr;
    }

    Errors::ValidationStatus SetVariableStatement::CompileDerived(QueryContext& context) {
        const auto& type = this->variable.GetType();

        auto validationStatus = Errors::ValidationStatus(context.GetAllocator());
        if (this->expression) {
            auto res = CompileExpression(context, this->expression);

            if (!res.IsOk()) return res;

            if (type != DataType::Null && !ValidateExpressionCoercionTypes(type, this->expression)) {
                validationStatus.code = Errors::ValidationError::Error;
                validationStatus.message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(this->expression),
                    type
                );

                return validationStatus;
            }

            if (type == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        context._scope.variables.ForceAdd(this->variable.GetNormalizedName(), type);
        return validationStatus;
    }

  constexpr Security::Permission SetVariableStatement::RequiredPermissions() const {
    return Constants::DB_WRITER_PERMISSIONS;
  }

  LogicalPlan * SetVariableStatement::ToLogical(QueryContext& context) {
    return context._compileContext.Allocate<LogicalDeclareVariable>(this->sessionId, this->variable, this->expression);
  }

  Errors::ValidationStatus CreateUserStatement::CompileDerived(QueryContext& context){
    if (this->username.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_USERNAME, context.GetAllocator());
    if (this->password.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_PASSWORD, context.GetAllocator());
    if (this->role.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_ROLE, context.GetAllocator());
    if (this->server->UserExists(this->username))
        return Errors::ValidationStatus::Error(Messages::USER_ALREADY_EXISTS, context.GetAllocator());
    if (!this->server->RoleExists(this->role))
        return Errors::ValidationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());

    return Errors::ValidationStatus(context.GetAllocator());
  }

  constexpr Security::Permission CreateUserStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

    LogicalPlan* CreateUserStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalCreateUser>(this->sessionId, this->username, this->password, this->role);
    }

    Errors::ValidationStatus GrantRoleStatement::CompileDerived(QueryContext& context){
        if (!this->server->UserExists(this->username))
            return Errors::ValidationStatus::Error(Messages::USER_DOES_NOT_EXIST, context.GetAllocator());
        if (!this->server->RoleExists(this->role))
            return Errors::ValidationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());
        return Errors::ValidationStatus(context.GetAllocator());
    }

    constexpr Security::Permission GrantRoleStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan * GrantRoleStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalGrantRole>(this->sessionId, this->username, this->role);
    }

    Errors::ValidationStatus DeleteStatement::CompileDerived(QueryContext& context){
        auto result = this->table->Validate(context, this->databaseId);

        if (!result.IsOk()) return result;

        if (this->where.expression == nullptr) return result;

        const auto columnsDict = this->catalog->SelectColumnsToDictionary(
            context.GetAllocator(),
            this->table->tableId
        );

        return result;
        // return this->where.expression->Validate(columnsDict);
    }

    constexpr Security::Permission DeleteStatement::RequiredPermissions() const{
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan * DeleteStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalDelete>(this->table, this->where.expression);
    }

    JoinStatement::JoinStatement() {
        this->type = JoinType::Inner;
        this->table = nullptr;
        this->expression = nullptr;
    }

    Errors::ValidationStatus JoinStatement::CompileDerived(QueryContext& context){
        return Errors::ValidationStatus(context.GetAllocator());
    }

    Errors::ValidationStatus JoinStatement::Validate(const QueryContext& context, const Int databaseId){
        this->databaseId = databaseId;

        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::MISSING_TABLE_IN_JOIN,
                context.GetAllocator()
            );

        return this->table->Validate(context, this->databaseId);
    }

  bool JoinStatement::IsRightJoin() const {
    return this->type == JoinType::Right;
  }

  bool JoinStatement::IsInnerJoin() const{
    return this->type == JoinType::Inner;
  }

  bool JoinStatement::IsFullOuterJoin() const{
    return this->type == JoinType::Full;
  }

  LogicalPlan * JoinStatement::ToLogical(QueryContext& context){
    return context._compileContext.Allocate<LogicalTableScan>(this->table, nullptr);
  }

  constexpr Security::Permission JoinStatement::RequiredPermissions() const{
    return Constants::DB_READER_PERMISSIONS;
  }

  // LogicalPlan * JoinStatement::ToLogical(CompileResult& context){
  //   return context._context.Allocate< LogicalJoin(this->table, this->joinType, this->table2, this->on.expression);
  // }

    StatementValidationScope::StatementValidationScope(
        const Dictionary<DataTypes::String, table_id_t> &tableAliasesDictionary,
        Dictionary<int, Dictionary<DataTypes::String, Headers::ColumnHeader>> &tablesColumnsDictionary,
        Statement *statement,
        int *indexPos
    ) {
        this->tableAliasesDictionary = &tableAliasesDictionary;
        this->tablesColumnsDictionary = &tablesColumnsDictionary;
        this->statement = statement;
        this->indexPos = indexPos;
    }

    DecimalType::DecimalType(){
        this->precision = INVALID_DECIMAL_PRECISION;
        this->scale = INVALID_DECIMAL_SCALE;
    }

    DecimalType::DecimalType(const TinyInt precision, const TinyInt scale){
        this->precision = precision;
        this->scale = scale;
    }

    bool DecimalType::Validate() const{
        if (this->precision == INVALID_DECIMAL_PRECISION
            || this->scale == INVALID_DECIMAL_SCALE
        ) return false;

        return (
            this->precision <= MAX_DECIMAL_PRECISION
            && this->scale <= this->precision
        );
    }

    ColumnType::ColumnType(){
        this->size = 0;
    }

    ColumnType::ColumnType(DataTypes::String&& name)
        : name(std::move(name)), size(0){}

    ColumnType::ColumnType(const DataTypes::String& name){
        this->name = name;
        this->size = 0;
    }

    ColumnType::ColumnType(const DataTypes::String& name, const Int size){
        this->name = name;
        this->size = size;
    }

    ColumnType::ColumnType(DataTypes::String& name, const Int size){
        this->name = std::move(name);
        this->size = size;
    }

    ColumnType::ColumnType(const DataTypes::String& name, const DecimalType decimal){
        this->size = 0;
        this->name = name;
        this->decimal = decimal;
    }

    ColumnType::ColumnType(DataTypes::String& name, const DecimalType decimal){
        this->name = std::move(name);
        this->decimal = decimal;
        this->size = 0;
    }

    Errors::ValidationStatus Identity::Validate(const QueryContext& context) const{
        if (this->incrementFactor <= 0)
            return Errors::ValidationStatus::Error(
                Messages::INVALID_INCREMENT_FACTOR,
                context.GetAllocator()
            );
        return Errors::ValidationStatus(context.GetAllocator());
    }

    bool NewColumn::HasIdentity()const{ return this->identity != nullptr;}

    OrderColumn::OrderColumn(){
        this->expression = nullptr;
        this->type = Constants::OrderType::ASCENDING;
    }

    OrderColumn::~OrderColumn() = default;

    AlterColumn::AlterColumn(){
        this->columnId = INVALID_COLUMN_ID;
        this->index = INVALID_ORDINAL_POS;
    }

    WhereClause::WhereClause() { this->expression = nullptr; }

    bool WhereClause::IsValid() const{
        return (this->expression == nullptr)
            || this->expression->IsLogical()
            || this->expression->IsBinary();
    }

    OrderByStatement::~OrderByStatement() = default;

    bool OrderByStatement::Validate(
        const DataStructures::PolymorphicArray<OrderColumn*>& selectColumns,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& columnsDict
    ){
        HashSet<DataTypes::String> selectColumnMap;
        // for (const auto& selectColumn : selectColumns) {
        //   selectColumnMap.Add(selectColumn->name.name);
        // }
        //
        // for(const auto& column : this->columns){
        //   if(!selectColumnMap.Contains(column->name.name)){
        //     cerr << "Column " << column->name.name << " does not exist on the statement." << endl;
        //     return false;
        //   }
        //
        //   Headers::ColumnHeader header;
        //   columnsDict.TryGetValue(column->name.name, header);
        //
        //   this->columnIndices.Push(header.ordinalPosition);
        // }

        return true;
  }

    DataSource::DataSource(const ::Memory::IAllocator* allocator) {
        this->databaseId = INVALID_DATABASE_ID;
        this->tableId = INVALID_TABLE_ID;
        this->schemaId = INVALID_SCHEMA_ID;
        this->ordinalPosition = INVALID_ORDINAL_POS;
        this->schema = DataTypes::String::FromView(Constants::DEFAULT_SCHEMA_NAME, allocator);
        this->server = &Network::Server::Get();
        this->catalog = &CoreEngine::SystemCatalog::Get();
    }

    DataTypes::String DataSource::GetAlias(const QueryContext& context) const{
        if (this->alias.Empty())
            return this->GetFullName(context);
        return this->alias;
    }

    DataTypes::String DataSource::GetFullName(const QueryContext& context) const {
        if (this->database.Empty())
            return DataTypes::String::Concat(
                context.GetAllocator(),
                this->schema,
                Messages::DOT,
                this->name
            );


        return DataTypes::String::Concat(
            context.GetAllocator(),
            this->database,
            Messages::DOT,
            this->schema,
            this->name
        );
    }

    Errors::ValidationStatus DataSource::Validate(const QueryContext& context, const Int selectedDatabaseId) {
        const auto tableHeader = (!this->database.Empty())
            ? this->catalog->SelectTable(
                context.GetAllocator(),
                this->database.ToView(),
                this->name.ToView()
            )
            : this->catalog->SelectTable(
                context.GetAllocator(),
                selectedDatabaseId,
                this->name.ToView(),
                this->schema.ToView()
            );

        if (tableHeader.id == INVALID_TABLE_ID){
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE(context.GetAllocator(), this->GetFullName(context))
            );
        }

        this->tableId = tableHeader.id;
        this->ordinalPosition = tableHeader.ordinalPosition;
        this->databaseId = tableHeader.databaseId;

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus DataSource::ValidateTableCreate(const QueryContext& context, const Int selectedDatabaseId){
        const auto tableHeader = (!this->database.Empty())
            ? this->catalog->SelectTable(
                context.GetAllocator(),
                this->database.ToView(),
                this->name.ToView()
            )
            : this->catalog->SelectTable(
                context.GetAllocator(),
                selectedDatabaseId,
                this->name.ToView(),
                this->schema.ToView()
            );

        if (tableHeader.id != INVALID_TABLE_ID)
            return Errors::ValidationStatus::Error(
            Messages::TABLE_ALREADY_EXISTS(context.GetAllocator(), this->GetFullName(context))
            );

        this->databaseId = selectedDatabaseId;
        return Errors::ValidationStatus::Ok();
    }

    CreateTableStatement::CreateTableStatement(){
        this->table = nullptr;
        this->constraint = nullptr;
    }

    CreateTableStatement::~CreateTableStatement() = default;

    Errors::ValidationStatus CreateTableStatement::CompileSchema(const QueryContext& context) const{
        const auto& schemasDict = this->catalog->SelectSchemasToDictionary(context.GetAllocator(), this->databaseId);
        Headers::SchemaHeader schemaHeader;

        this->table->schema.ToLowerInPlace();
        if (!schemasDict.TryGetValue(this->table->schema, schemaHeader))
            return Errors::ValidationStatus::Error(
                Messages::SCHEMA_DOES_NOT_EXIST(context.GetAllocator(), this->table->schema)
            );

        this->table->schemaId = schemaHeader.id;
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CreateTableStatement::CompileColumnExpression(
        const QueryContext& context,
        NewColumn*& column,
        Dictionary<DataTypes::String, column_index_t>& columnNamesToIndexes,
        bool& primaryKeyFound,
        column_index_t& index
    ){
        UnsignedSmallInt columnSize;
        std::ostringstream os;

        column->type.name.ToLowerInPlace();
        if (!ColumnTypeSizes.TryGetValue(column->type.name.ToView(), columnSize)) {
            return Errors::ValidationStatus::Error(
                Messages::DATATYPE_DOES_NOT_EXIST(context.GetAllocator(), column->type.name)
            );
        }

        if (columnSize != 0)
        column->type.size = columnSize;

        const auto& dataType = ColumnTypesDictionary.Get(column->type.name.ToView());

        if (dataType == DataType::Decimal) {
            if (!column->type.decimal.Validate()) {
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_DECIMAL_DECLARATION,
                    context.GetAllocator()
                );
            }

            column->type.size = DataTypes::Decimal::Size(column->type.decimal.precision);
        }

        column->index = index++;

        columnNamesToIndexes.Add(column->name.name, column->index);

        if(column->isPrimaryKey && primaryKeyFound){
            return Errors::ValidationStatus::Error(
                Messages::MULTIPLE_PRIMARY_KEYS,
                context.GetAllocator()
            );
        }

        if(column->HasIdentity()) {
            auto result = column->identity->Validate(context);
            if (!result.IsOk()) return result;
        }

        if (column->isPrimaryKey) {
            this->primaryKey.Push(column->index);
            primaryKeyFound = true;
        }

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CreateTableStatement::CompileDerived(QueryContext& context){
        auto result = this->table->ValidateTableCreate(context, this->databaseId);
        if (!result.IsOk()) return result;

        result = this->CompileSchema(context);
        if (!result.IsOk()) return result;

        this->columns.TrySetAllocator(context.GetAllocator());
        this->primaryKey.TrySetAllocator(context.GetAllocator());

        column_index_t indexPosition = 0;
        bool primaryKeyFound = false;
        Dictionary<DataTypes::String, column_index_t> columnNamesToIndexes;

        for (auto* column: this->columns)
            this->CompileColumnExpression(
                context,
                column,
                columnNamesToIndexes,
                primaryKeyFound,
                indexPosition
            );

        if (this->constraint == nullptr)return Errors::ValidationStatus::Ok();

        if (primaryKeyFound) {
            return Errors::ValidationStatus::Error(
                Messages::PRIMARY_KEY_AND_CONSTRAINT_DECLARED,
                context.GetAllocator()
            );
        }

        //primary key will be clear for sure here
        for (const auto& column: this->constraint->columns)
            this->primaryKey.Push(columnNamesToIndexes[column.name]);

        return Errors::ValidationStatus::Ok();
    }

    LogicalPlan * CreateTableStatement::ToLogical(QueryContext& context){
        auto constraintName = (this->constraint == nullptr)
            ? DataTypes::String::Null()
            : this->constraint->name;

        return context._compileContext.Allocate<LogicalTableCreate>(
            this->sessionId,
            this->table,
            this->columns,
            this->primaryKey,
            constraintName
        );
    }

    constexpr Security::Permission CreateTableStatement::RequiredPermissions() const{
        return Constants::DB_OWNER_PERMISSIONS;
    }

    SelectStatement::SelectStatement(const ::Memory::IAllocator* allocator)
        : Statement(), columnHeaders(allocator), results(allocator), joins(allocator) {
        this->top = INVALID_TOP;
        this->distinct = false;
        this->orderBy = nullptr;
    }

    SelectStatement::~SelectStatement() = default;

    Dictionary<DataTypes::String, column_index_t> SelectStatement::CreatePostProjectionIndicesDictionary() const{
        Dictionary<DataTypes::String, column_index_t> dict;

        for (int i = 0;i < this->results.Size(); i++) {
            const auto& resultExpr = this->results[i];

            if (resultExpr->name.Empty()) continue;
            dict.Add(resultExpr->name, i);
        }

        return dict;
    }

    bool SelectStatement::HasTopStatement() const{ return this->top != INVALID_TOP; }

    bool SelectStatement::HasJoins()const{ return !this->joins.Empty(); }

    bool SelectStatement::HasWhere() const{ return this->where.expression != nullptr; }

    bool SelectStatement::IsConstant() const{ return this->table == nullptr; }

    Errors::ValidationStatus SelectStatement::CompileNoTableStatement(QueryContext& context){
        for (auto& resultExpr : this->results){
            auto exprResult = CompileExpression(context, resultExpr);

            if (!exprResult.IsOk()) return exprResult;
        }

        if (this->HasJoins())
            return Errors::ValidationStatus::Error(
                Messages::JOIN_WITH_NO_BASE_TABLE_SELECT,
                context.GetAllocator()
            );

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus SelectStatement::Compile(
        QueryContext& context,
        Dictionary<DataTypes::String, table_id_t> &tableAliasesDictionary
    ){
        //Add Base Table to the dictionaries
        tableAliasesDictionary.Add(this->table->GetAlias(context), this->table->tableId);
        this->tableColumnsDictionary.Add(
            this->table->tableId,
            this->catalog->SelectColumnsToDictionary(context.GetAllocator(), this->table->tableId)
        );

        //Add all the join tables to the dictionaries
        for (const auto& join: this->joins) {
            tableAliasesDictionary.Add(
                join->table->GetAlias(context),
                join->table->tableId
            );

            this->tableColumnsDictionary.Add(
                join->table->tableId,
                this->catalog->SelectColumnsToDictionary(context.GetAllocator(), join->table->tableId)
            );
        }

        auto statementValidationScope = StatementValidationScope(
            tableAliasesDictionary,
            this->tableColumnsDictionary,
            this
        );

        //start resolving aliases
        for (int i = 0; i < this->results.Size(); i++) {
            statementValidationScope.indexPos = &i;
            auto expressionResult = CompileExpression(context, statementValidationScope, this->results[i]);

            if (!expressionResult.IsOk()) return expressionResult;
        }

        auto result = this->CompileWhereClause(context, statementValidationScope);
        if (!result.IsOk()) return result;

        //validate join expressions
        statementValidationScope.indexPos = nullptr;
        for (const auto& join: this->joins) {
            auto expressionResult = CompileExpression(context, statementValidationScope, join->expression);

            if (!expressionResult.IsOk()) return expressionResult;
        }

        if (this->orderBy == nullptr)
            return Errors::ValidationStatus::Ok();

        Dictionary<DataTypes::String, const Expressions::Expression*> postProjectionAliases;

        for (const auto* resultExpr : this->results) {
            if (resultExpr->name.Empty()) continue;

            if (postProjectionAliases.Contains(resultExpr->name)) {
                return Errors::ValidationStatus::Error(
                    Messages::DUPLICATE_RESULT_COLUMN_NAMES,
                    context.GetAllocator()
                );
            }

            postProjectionAliases.Add(resultExpr->name, resultExpr);
        }

        for (const auto& column: this->orderBy->columns) {
            auto postProjectionExpr = CompilePostProjectionExpression(
                context,
                column->expression,
                postProjectionAliases
            );
            if (!postProjectionExpr.IsOk()) return postProjectionExpr;
        }

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus SelectStatement::CompileWhereClause(
        QueryContext &context,
        StatementValidationScope& statementValidationScope
    ) {
        if (!this->HasWhere()) return Errors::ValidationStatus::Ok();

        if (!this->where.IsValid())
            return Errors::ValidationStatus::Error(
                Messages::INVALID_WHERE_CLAUSE,
                context.GetAllocator()
            );

        auto expressionResult = CompileExpression(context, statementValidationScope, this->where.expression);
        if (!expressionResult.IsOk()) return expressionResult;

        if (!ValidateExpressionCoercionTypes(DataType::Bool, this->where.expression))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(this->where.expression));

        return Errors::ValidationStatus::Ok();
    }

    LogicalPlan* SelectStatement::BuildTableScanPlan(
        const QueryContext& context,
        DataSource* table,
        const PredicatePushDownResult& predicatesResult
    ){
        return context._compileContext.Allocate<LogicalTableScan>(table, predicatesResult.PushDownFilter(table->tableId));
    }

    LogicalPlan* SelectStatement::BuildJoinsPlan(
        const QueryContext& context,
        const JoinOrderAnalyzeResult& joinReorderResult,
        const PredicatePushDownResult& predicatesResult
    ) const{
        const auto leftTableId = this->table->tableId;
        auto* current = SelectStatement::BuildTableScanPlan(context, this->table, predicatesResult);

        for (const auto& join : joinReorderResult.orderedJoins){
            auto* right = SelectStatement::BuildTableScanPlan(context, join->table, predicatesResult);

            current = context._compileContext.Allocate<LogicalJoin>(
                current,
                right,
                join->expression,
                join->type,
                leftTableId,
                join->table->tableId
            );
        }

        return current;
  }

    Dictionary<Int, column_index_t> SelectStatement::BuildColumnsIndicesDictionary(
        const QueryContext& context,
        const DataStructures::PolymorphicArray<table_id_t>& joinOrder
    ) const{
        Dictionary<Int, column_index_t> result;
        column_index_t columnIndex = 0;

        for (const auto& tableId: joinOrder) {
        //TODO cache them at the beginning
            const auto& columns = this->catalog->SelectColumns(context.GetAllocator(), tableId);
            for (const auto &column : columns) {
                if (result.Contains(column.id)) continue;
                result.Add(column.id, columnIndex + column.ordinalPosition);
            }

            columnIndex += columns.Size();
        }

        return result;
    }

    void SelectStatement::AssignColumnsToIndices(
        const QueryContext& context,
        const DataStructures::PolymorphicArray<table_id_t>& order
    )const {
        const auto columnIndicesDictionary = this->BuildColumnsIndicesDictionary(context, order);
        for (const auto& resultExpr : this->results)
            AssignColumnIndicesToExpression(columnIndicesDictionary, resultExpr);
        if (this->where.expression != nullptr)
            AssignColumnIndicesToExpression(columnIndicesDictionary, this->where.expression);
        for (const auto* join : this->joins)
            AssignColumnIndicesToExpression(columnIndicesDictionary, join->expression);
    }

    void SelectStatement::BuildOrderByStatement(
        LogicalPlan*& current,
        const Dictionary<DataTypes::String, column_index_t>& postProjectionIndicesDictionary
    ) const{
        if(this->orderBy == nullptr) return;

        for (const auto& column : this->orderBy->columns)
            AssignPostProjectionIndicesToExpression(postProjectionIndicesDictionary, column->expression);

        current = new LogicalOrder(current, this->orderBy->columns);
    }

    Errors::ValidationStatus SelectStatement::CompileDerived(QueryContext& context){
        if (!this->joins.Empty() && this->table == nullptr) {
            return Errors::ValidationStatus::Error(
             Messages::JOIN_WITH_NO_BASE_TABLE_SELECT,
             context.GetAllocator()
            );
        }

        //resolve expressions here since no column is to be used
        if (this->table == nullptr) return this->CompileNoTableStatement(context);

        auto tableResult = this->table->Validate(context, this->databaseId);
        if (!tableResult.IsOk()) return tableResult;

        Dictionary<DataTypes::String, table_id_t> aliasesDictionary;

        for (const auto& join: this->joins) {
            auto joinResult = join->Validate(context, this->databaseId);
            if (!joinResult.IsOk()) return joinResult;
        }

        return this->Compile(context, aliasesDictionary);
    }

    constexpr Security::Permission SelectStatement::RequiredPermissions() const{
        return Constants::DB_READER_PERMISSIONS;
    }

    LogicalPlan * SelectStatement::ToLogical(QueryContext& context){
        if (this->IsConstant())
            return context._compileContext.Allocate<LogicalProject>(nullptr, this->results, this->columnHeaders);

        const Optimizer optimizer(context);
        const auto joinReorderResult = optimizer.DetermineJoinOrder(this);

        auto predicatesResult = optimizer.PushDownPredicates(
            joinReorderResult.order,
            this->where.expression,
            joinReorderResult.orderedJoins
        );

        this->AssignColumnsToIndices(context, joinReorderResult.order);
        auto* current = this->BuildJoinsPlan(context, joinReorderResult, predicatesResult);

        if (predicatesResult.remainingPredicate != nullptr)
            current = context._compileContext.Allocate<LogicalFilter>(current, predicatesResult.remainingPredicate);

        const auto postProjectionIndicesDictionary = this->CreatePostProjectionIndicesDictionary();

        current = context._compileContext.Allocate<LogicalProject>(current, this->results, this->columnHeaders);

        this->BuildOrderByStatement(current, postProjectionIndicesDictionary);

        if (this->distinct)
            current = context._compileContext.Allocate<LogicalDistinct>(current);

        if (this->HasTopStatement())
            current = context._compileContext.Allocate<LogicalTop>(current, this->top);

        return current;
    }

    Errors::ValidationStatus CreateDbStatement::CompileDerived(QueryContext& context){
        if (this->catalog->DatabaseExists(context.GetAllocator(), this->name.ToView())) {
            return Errors::ValidationStatus::Error(
                Messages::DATABASE_ALREADY_EXISTS(context.GetAllocator(), this->name)
            );
        }

        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission CreateDbStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan* CreateDbStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalCreateDatabase>(this->sessionId, this->name);
    }

    Errors::ValidationStatus DropDbStatement::CompileDerived(QueryContext& context){
        const auto database = this->catalog->SelectDatabase(context.GetAllocator(), this->name.ToView());

        if (database.name.Empty()) {
            return Errors::ValidationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_DROP(
                    context.GetAllocator(),
                    this->name.ToView()
                )
            );
        }

        if (database.isSystem) {
            return Errors::ValidationStatus::Error(
                Messages::CANNOT_DROP_SYSTEM_DATABASE(
                    context.GetAllocator(),
                    this->name.ToView()
                )
            );
        }

        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission DropDbStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan * DropDbStatement::ToLogical(QueryContext& context){
        return nullptr;
    }

    Errors::ValidationStatus UseDatabaseStatement::CompileDerived(QueryContext& context){
        const auto dbHeader = this->catalog->SelectDatabase(
            context.GetAllocator(),
            this->name.ToView()
        );

        if (dbHeader.id == INVALID_DATABASE_ID) {
            return Errors::ValidationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_USE(context.GetAllocator(), this->name.ToView())
            );
        }

        this->databaseId = dbHeader.id;
        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission UseDatabaseStatement::RequiredPermissions() const{
        return Constants::GUEST_PERMISSIONS;
    }

    LogicalPlan * UseDatabaseStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalUseDatabase>(this->sessionId, this->databaseId);
    }

    InsertStatement::~InsertStatement() = default;

    void InsertStatement::InsertDefaultValuesForMissingColumns(
        const QueryContext& context,
        const Headers::ColumnHeader &header,
        const Headers::DefaultValuesHeader& defaultValue
    ){
        this->columns.Push(ColumnName{
            .name = header.name,
            .alias = header.name,
            .tableId = this->table->tableId,
            .columnId = header.id,
            .index = static_cast<column_index_t>(header.ordinalPosition),
            .returnType = static_cast<DataType>(header.dataType),
            }
        );

        //Insert the default value
        for (auto& [insertColumns] : this->values) {
            const auto* data = reinterpret_cast<const unsigned char*>(defaultValue.value.Data());

            auto value = Value::FromExternalStorage(
                data,
                defaultValue.value.Size(),
                static_cast<DataType>(header.dataType),
                context._compileContext.GetAllocator(),
                0
            );

            insertColumns.Push(context._compileContext.Allocate<Expressions::ConstantExpression>(value));
        }
    }

    void InsertStatement::InsertNullValuesForMissingColumns(const Headers::ColumnHeader& header){
        this->columns.Push(ColumnName{
            .name = header.name,
            .alias = header.name,
            .tableId = this->table->tableId,
            .columnId = header.id,
            .index = static_cast<column_index_t>(header.ordinalPosition),
            .returnType = static_cast<DataType>(header.dataType),
            }
        );

        for (auto& [insertColumns] : this->values)
            insertColumns.Push(new Expressions::ConstantExpression(Value::Null()));
    }

    Errors::ValidationStatus InsertStatement::ValidateReturnType(
        const QueryContext& context,
        const Expressions::Expression* expression,
        const DataTypes::String& columnName
    ) const{
        const auto valueType = Expressions::GetExpressionReturnType(expression);

        const auto& columnsDictionary = this->tableColumnsDictionary.Get(this->table->tableId);

        const auto columnNameToLower = columnName.ToLower();
        const auto& columnHeader = columnsDictionary.Get(columnNameToLower);

        const auto columnType = static_cast<DataType>(columnHeader.dataType);

        if (DataTypes::Coercions::IsCoercionAllowed(
            valueType,
            columnType
        )) return Errors::ValidationStatus::Ok();

        if (expression->IsConstant()) {
            auto* constantExpr = expression->AsConstant();

            if (constantExpr->value.IsNull()) {
                if (columnHeader.isNullable) return Errors::ValidationStatus::Ok();

                return Errors::ValidationStatus::Error(
            Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                        context.GetAllocator(),
                        columnHeader.name
                    )
                );
            }

            if (DataTypes::Coercions::CanBeParsedToType(columnType, constantExpr->value))
                return Errors::ValidationStatus::Ok();
        }

        return Errors::ValidationStatus::Error(
            Messages::CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
                context.GetAllocator(),
                columnName,
                SqlTypesString[static_cast<Int>(columnType)],
                SqlTypesString[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::ValidationStatus InsertStatement::ValidateSelectStatement(QueryContext& context)const{
        if (this->selectStatement == nullptr)
            return Errors::ValidationStatus::Ok();

        if (this->selectStatement->results.Size() != this->columns.Size())
            return Errors::ValidationStatus::Error(
                Messages::INSERT_STATEMENT_INVALID_NUMBER_OF_ARGUMENTS_ON_SUB_SELECT,
                context.GetAllocator()
            );

        this->selectStatement->databaseId = this->databaseId;

        auto selectStatus = this->selectStatement->CompileDerived(context);
        if (!selectStatus.IsOk()) return selectStatus;

        for (int i = 0;i < this->selectStatement->results.Size();i++) {
            const auto& resultExpression = this->selectStatement->results[i];

            auto returnTypeStatus = this->ValidateReturnType(context, resultExpression, this->columns[i].name);
            if (!returnTypeStatus.IsOk()) return returnTypeStatus;
        }

        return Errors::ValidationStatus::Ok();
    }

    bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

    Errors::ValidationStatus InsertStatement::ResolveAliases(QueryContext& context){
        const Dictionary<DataTypes::String, table_id_t> tableAliasesDictionary{
        std::pair(this->table->GetAlias(context), this->table->tableId)
        };

        auto statementValidationScope = StatementValidationScope(
            tableAliasesDictionary,
            this->tableColumnsDictionary,
            this,
            nullptr
        );

        for (auto& [insertColumns] : this->values) {
            for (int i = 0;i < insertColumns.Size(); i++) {
                auto& value = insertColumns[i];

                auto expressionStatus = CompileExpression(context, statementValidationScope, value);
                if (!expressionStatus.IsOk()) return expressionStatus;

                auto returnTypeStatus = this->ValidateReturnType(context, value, this->columns[i].name);
                if (!returnTypeStatus.IsOk()) return returnTypeStatus;
            }
        }

        return Errors::ValidationStatus::Ok();
    }

  //TODO validate length of columns to match max record_size from master DB
    Errors::ValidationStatus InsertStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::NO_TABLE_SPECIFIED,
                context.GetAllocator()
            );

        auto tableStatus = this->table->Validate(context, this->databaseId);
        if (!tableStatus.IsOk()) return tableStatus;

        this->columnIndices.TrySetAllocator(context.GetAllocator());

        const auto columnsDict = this->catalog->SelectColumnsToDictionary(context.GetAllocator(), this->table->tableId);

        this->tableColumnsDictionary.Add(this->table->tableId, columnsDict);

        const auto identityColumns =
            this->catalog->SelectIdentityColumnsByTableIdToDictionary(
                context.GetAllocator(),
                this->table->tableId
            );

        //validate insert columns existence
        HashSet<Int> statementColumns;
        for (auto& column : this->columns) {
            Headers::ColumnHeader header;

            //check if columns exist on the table
            const auto columnNameToLower = column.name.ToLower();
            if (!columnsDict.TryGetValue(columnNameToLower, header)) {
                return Errors::ValidationStatus::Error(
                    Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                        context.GetAllocator(),
                        this->table->GetAlias(context),
                        column.name
                    )
                );
            }

            //check if the specified column is an identity column
            if (identityColumns.Contains(header.id)) {
                return Errors::ValidationStatus::Error(
                    Messages::IDENTITY_COLUMN_ON_INSERT,
                    context.GetAllocator()
                );
            }

            column.index = header.ordinalPosition;
            column.columnId = header.id;
            statementColumns.Add(header.id);
            this->columnIndices.Push(header.ordinalPosition);
        }

        for (const auto& header : columnsDict | std::views::values) {
            if (header.isSystem
                || identityColumns.Contains(header.id)
                || statementColumns.Contains(header.id)
            ) continue;

            this->columnIndices.Push(static_cast<column_index_t>(header.ordinalPosition));

            //Insert the null value
            if (header.isNullable) {
                this->InsertNullValuesForMissingColumns(header);
                continue;
            }

            const auto defaultValue = this->catalog->SelectDefaultValueByColumnId(context.GetAllocator(), header.id);

            if (defaultValue.columnId == INVALID_COLUMN_ID) {
                return Errors::ValidationStatus::Error(
                    Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                        context.GetAllocator(),
                        header.name
                    )
                );
            }

            this->InsertDefaultValuesForMissingColumns(context, header, defaultValue);
        }

        return this->HasSelectStatement()
            ? this->ValidateSelectStatement(context)
            : this->ResolveAliases(context);
    }

    constexpr Security::Permission InsertStatement::RequiredPermissions() const{
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan* InsertStatement::ToLogical(QueryContext& context) {
        auto* logicalSelect = this->HasSelectStatement()
            ? this->selectStatement->ToLogical(context)
            : nullptr;

        return context._compileContext.Allocate<LogicalInsert>(
            this->table,
            this->values,
            logicalSelect,
            this->columnIndices
        );
    }

    Errors::ValidationStatus CreateSchemaStatement::CompileDerived(QueryContext& context){
        if (this->catalog->SchemaExists(context.GetAllocator(), this->databaseId, this->name.ToView())) {
            return Errors::ValidationStatus::Error(
                Messages::SCHEMA_ALREADY_EXISTS(context.GetAllocator(), this->name)
            );
        }

        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission CreateSchemaStatement::RequiredPermissions() const{
        return Constants::DB_OWNER_PERMISSIONS;
    }

    LogicalPlan * CreateSchemaStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalSchemaCreate>(
            this->sessionId,
            this->databaseId,
            this->name
        );
    }

    UpdateColumn::UpdateColumn()
        : value(nullptr), name(DataTypes::String::Null()){}

  UpdateColumn::~UpdateColumn() = default;

    Errors::ValidationStatus UpdateStatement::ValidateReturnType(
        const QueryContext& context,
        const UpdateColumn* update
    ) const{
        const auto valueType = Expressions::GetExpressionReturnType(update->value);
        if (
            DataTypes::Coercions::IsCoercionAllowed(
            valueType,
            update->name.returnType
            )
        ) return Errors::ValidationStatus::Ok();

        if (update->value->IsConstant()) {
            const auto* constantExpr = update->value->AsConstant();

            if (constantExpr->value.IsNull()) {
                const auto& columns = this->tableColumnsDictionary.Get(this->table->tableId);

                if (columns.Get(update->name.name).isNullable)
                    return Errors::ValidationStatus::Ok();

                return Errors::ValidationStatus::Error(
                    Messages::COLUMN_DOES_NOT_ALLOW_NULLS(context.GetAllocator(), update->name.name)
                );
            }

            if (DataTypes::Coercions::CanBeParsedToType(update->name.returnType, constantExpr->value))
                return Errors::ValidationStatus::Ok();
        }

        return Errors::ValidationStatus::Error(
            Messages::CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
                context.GetAllocator(),
                update->name.name,
                SqlTypesString[static_cast<Int>(update->name.returnType)],
                SqlTypesString[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::ValidationStatus UpdateStatement::ResolveAliases(
        QueryContext& context,
        Dictionary<DataTypes::String, table_id_t> &tableAliasesDictionary
    ){
        //Add Base Table to the dictionaries
        tableAliasesDictionary.Add(this->table->GetAlias(context), this->table->tableId);
        this->tableColumnsDictionary.Add(this->table->tableId, this->catalog->SelectColumnsToDictionary(context.GetAllocator(), this->table->tableId));

        auto statementValidationScope = StatementValidationScope(
            tableAliasesDictionary,
            this->tableColumnsDictionary,
        this,
        nullptr
        );

        //start resolving aliases
        for (const auto& update : this->updates) {
            auto columnAliasStatus = CompileColumnExpression(context, update->name, statementValidationScope);
            if (!columnAliasStatus.IsOk())
                return columnAliasStatus;

            auto expressionStatus = CompileExpression(context, statementValidationScope, update->value);
            if (!expressionStatus.IsOk())
                return expressionStatus;

            auto returnTypeResult = this->ValidateReturnType(context, update);
            if (!returnTypeResult.IsOk())
                return returnTypeResult;

            update->value->SetIndex(update->name.index);
        }

        if (this->where.expression == nullptr)
            return Errors::ValidationStatus::Ok();

        if (!this->where.IsValid())
            return Errors::ValidationStatus::Error(
                Messages::INVALID_WHERE_CLAUSE,
                context.GetAllocator()
            );

        return CompileExpression(context, statementValidationScope, this->where.expression);
    }


    Errors::ValidationStatus UpdateStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::NO_TABLE_SPECIFIED,
                context.GetAllocator()
            );

        auto tableStatus = this->table->Validate(context, this->databaseId);
        if (!tableStatus.IsOk()) return tableStatus;

        Dictionary<DataTypes::String, table_id_t> aliasesDictionary;
        return this->ResolveAliases(context, aliasesDictionary);
    }

    constexpr Security::Permission UpdateStatement::RequiredPermissions() const{
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan* UpdateStatement::ToLogical(QueryContext& context){
        DataStructures::PolymorphicArray<Expressions::Expression*> expressions(context.GetAllocator(), this->updates.Size());
        for (const auto& update : this->updates)
            expressions.Push(update->value);

        return context._compileContext.Allocate<LogicalUpdate>(
            this->table,
            expressions,
            this->where.expression
        );
    }

    Errors::ValidationStatus CreateIndexStatement::CompileDerived(QueryContext& context){
        auto tableStatus = this->table->Validate(context, this->databaseId);
        if (!tableStatus.IsOk()) return tableStatus;

        this->columns.TrySetAllocator(context.GetAllocator());
        this->columnIndices.TrySetAllocator(context.GetAllocator());

        const auto columnsDict = this->catalog->SelectColumnsToDictionary(context.GetAllocator(), this->table->tableId);

        for(auto& column: this->columns) {
            Headers::ColumnHeader header;
            if (columnsDict.TryGetValue(column, header)) {
                this->columnIndices.Push(header.ordinalPosition);
                continue;
            }

            return Errors::ValidationStatus::Error(
        Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                    context.GetAllocator(),
                    column,
                    this->table->GetAlias(context)
                )
            );
        }

        const auto indexes = this->catalog->SelectIndexes(context.GetAllocator(), this->table->tableId);

        for (const auto& index: indexes) {
            const auto indexedColumns = this->catalog->SelectIndexColumnsByIndexIdToDictionary(
                context.GetAllocator(),
                index.id
            );

            if (index.name == this->name) {
                return Errors::ValidationStatus::Error(
                    Messages::INDEX_EXISTS(
                        context.GetAllocator(),
                        this->name
                    )
                );
            }
            //check if identical index exists (no need for a duplicate).
        }
        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission CreateIndexStatement::RequiredPermissions() const{
        return Constants::DB_OWNER_PERMISSIONS;
    }

    LogicalPlan* CreateIndexStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalIndexCreate>(
            this->sessionId,
            this->table,
            this->name,
            this->columnIndices
        );
    }

    Errors::ValidationStatus AlterTableStatement::CompileAddColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
    )const{
        auto* newColumn = this->column.newColumn;
        const auto columnNameToLower = newColumn->name.name.ToLower();
        if (headers.Contains(columnNameToLower)) {
            return Errors::ValidationStatus::Error(
                Messages::COLUMN_ALREADY_EXISTS_ON_TABLE(
                        context.GetAllocator(),
                        this->table->GetAlias(context),
                        newColumn->name.name
                )
            );
        }

        if (!newColumn->isNullable
            && newColumn->defaultValue.IsNull()
        ) {
            return Errors::ValidationStatus::Error(
                Messages::DEFAULT_VALUE_NULL_ON_NOT_NULL_COLUMN,
                context.GetAllocator()
            );
        }

        newColumn->index = headers.size();

        DataType columnType;
        const auto columnTypeToLower = newColumn->type.name.ToLower();
        const auto columnTypeView = columnTypeToLower.ToView();
        if (!ColumnTypesDictionary.TryGetValue(columnTypeView, columnType)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_COLUMN_TYPE_SPECIFIED(
                    context.GetAllocator(),
                    newColumn->type.name,
                    columnTypeView
                )
            );
        }

        const auto recordSize = ColumnTypeSizes.Get(&columnTypeView);

        if (recordSize != 0)
            newColumn->type.size = recordSize;

        if (columnType == DataType::Decimal) {
            if (!newColumn->type.decimal.Validate()) {
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_DECIMAL_DECLARATION,
                    context.GetAllocator()
                );
            }

            newColumn->type.size = DataTypes::Decimal::Size(newColumn->type.decimal.precision);
        }

        //TODO Check this
        // this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus AlterTableStatement::CompileAlterColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
    )const{
        Headers::ColumnHeader header;
        std::ostringstream os;

        auto* alterColumn = this->column.alterColumn;

        const auto columnNameToLower = alterColumn->name.name.ToLower();
        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::ValidationStatus::Error(
        Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                    context.GetAllocator(),
                    this->table->GetAlias(context),
                    alterColumn->name.name
                )
            );
        }

        const auto columnTypeToLower = alterColumn->type.name.ToLower();
        const auto columnTypeView = columnTypeToLower.ToView();
        DataType columnType;
        if (!ColumnTypesDictionary.TryGetValue(columnTypeView, columnType)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_COLUMN_TYPE_SPECIFIED(
                    context.GetAllocator(),
                    alterColumn->type.name,
                    columnTypeView
                )
            );
        }

        if (
            (
                PipelineConstants::ValidTableStringConversions.Contains(columnType)
                && !PipelineConstants::ValidTableStringConversions.Contains(static_cast<DataType>(header.dataType))
            )
            || (
                PipelineConstants::ValidTableIntegerConversions.Contains(columnType)
                && !PipelineConstants::ValidTableIntegerConversions.Contains(static_cast<DataType>(header.dataType))
            )
        ){
            return Errors::ValidationStatus::Error(
                Messages::CANNOT_ALTER_COLUMN_TO_TYPE(
                        context.GetAllocator(),
                        alterColumn->name.name,
                        SqlTypesString[header.dataType],
                        alterColumn->type.name.ToView()
                )
            );
        }

        if (header.recordSize > alterColumn->type.size) {

            return Errors::ValidationStatus::Error(
                Messages::CANNOT_ALTER_COLUMN_TO_NEW_SIZE(
                    context.GetAllocator(),
                    alterColumn->name.name,
                    SqlTypesString[header.dataType],
                    alterColumn->type.size
                )
            );
        }

        alterColumn->columnId = header.id;
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus AlterTableStatement::CompileDropColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
    )const{
        Headers::ColumnHeader header;

        auto* dropColumn = this->column.dropColumn;
        const auto columnNameToLower = dropColumn->name.name.ToLower();
        const auto columnNameView = columnNameToLower.ToView();

        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::ValidationStatus::Error(
            Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                        context.GetAllocator(),
                        this->table->GetAlias(context),
                        dropColumn->name.name
                )
            );
        }

        //validate no index or constraint uses it
        const auto constraints = this->catalog->SelectConstraints(context.GetAllocator(), this->table->tableId);

        for (const auto& constraint: constraints) {
            const auto columns = this->catalog->SelectConstraintColumnsByConstraintIdToDictionary(
                context.GetAllocator(),
                constraint.constraintId
            );

            if (columns.Contains(header.id)) {
                return Errors::ValidationStatus::Error(
                    Messages::CANNOT_DROP_COLUMN_HAS_CONSTRAINTS(
                        context.GetAllocator(),
                        dropColumn->name.name,
                        constraint.name
                    )
                );
            }
        }

        dropColumn->index = header.ordinalPosition;
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus AlterTableStatement::CompileRenameColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
    )const{
        Headers::ColumnHeader header;

        auto* renameColumn = this->column.renameColumn;
        const auto columnNameToLower = renameColumn->oldName.name.ToLower();

        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::ValidationStatus::Error(
            Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                        context.GetAllocator(),
                        this->table->GetAlias(context),
                        renameColumn->oldName.name
                )
            );
        }

        renameColumn->columnId = header.id;
        renameColumn->ordinalPosition = header.ordinalPosition;

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus AlterTableStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::NO_TABLE_SPECIFIED,
                context.GetAllocator()
            );

        auto tableStatus = this->table->Validate(context, this->databaseId);
        if (!tableStatus.IsOk()) return tableStatus;

        const auto columnsDict = this->catalog->SelectColumnsToDictionary(context.GetAllocator(), this->table->tableId);

        //validate by type
        switch (this->type) {
            case Constants::AlterTableType::AddColumn:
                return this->CompileAddColumn(context, columnsDict);
            case Constants::AlterTableType::AlterColumn:
                return this->CompileAlterColumn(context, columnsDict);
            case Constants::AlterTableType::DropColumn:
                return this->CompileDropColumn(context, columnsDict);
            case Constants::AlterTableType::RenameColumn:
                return this->CompileRenameColumn(context, columnsDict);
            default:
                return Errors::ValidationStatus::Error(
                    Messages::UNKNOWN_OPERATION,
                    context.GetAllocator()
                );
        }
    }

    constexpr Security::Permission AlterTableStatement::RequiredPermissions() const{
        return Constants::DB_OWNER_PERMISSIONS;
    }

    LogicalPlan * AlterTableStatement::ToLogical(QueryContext& context){
        switch (this->type) {
        case Constants::AlterTableType::AddColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                this->sessionId,
                this->table,
                this->type,
                this->column.newColumn
            );
        case Constants::AlterTableType::AlterColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                this->sessionId,
                this->table,
                this->type,
                this->column.alterColumn
            );
        case Constants::AlterTableType::RenameColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                this->sessionId,
                this->table,
                this->type,
                this->column.renameColumn
            );
        case Constants::AlterTableType::DropColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                this->sessionId,
                this->table,
                this->type,
                this->column.dropColumn
            );
        default:
            return nullptr;
        }
    }

    Errors::ValidationStatus CompileExpression(QueryContext& context, Expressions::Expression*& expression){
        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                return CompileBinaryExpression(context, expression->AsBinary(), expression);
            case Expressions::ExpressionType::Logical:
                return CompileLogicalExpression(context, expression->AsLogical(), expression);
            case Expressions::ExpressionType::Branch:
                return CompileBranchExpression(context, expression->AsBranch(), expression);
            case Expressions::ExpressionType::Function:
                return CompileFunctionExpression(context, expression->AsFunction(), expression);
            case Expressions::ExpressionType::Column:
                return CompileColumnExpression(context, expression->AsColumn());
            case Expressions::ExpressionType::Variable:
                return CompileVariableExpression(context, expression->AsVariable());
            case Expressions::ExpressionType::Constant:
                return CompileConstantExpression(expression->AsConstant());
            case Expressions::ExpressionType::Json:
                return CompileJsonExpression(context, expression->AsJson());
            case Expressions::ExpressionType::Expression:
            default:
                break;
        }

        return Errors::ValidationStatus::Error(
         Messages::UNKNOWN_OPERATION,
            context.GetAllocator()
        );
  }

    Errors::ValidationStatus CompileExpression(
        QueryContext& context,
        StatementValidationScope& statementValidationScope,
        Expressions::Expression*& expression
    ){
        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                return CompileBinaryExpression(context, expression->AsBinary(), expression, statementValidationScope);
            case Expressions::ExpressionType::Logical:
                return CompileLogicalExpression(context, expression->AsLogical(), expression, statementValidationScope);
            case Expressions::ExpressionType::Branch:
                return CompileBranchExpression(context, expression->AsBranch(), expression, statementValidationScope);
            case Expressions::ExpressionType::Function:
                return CompileFunctionExpression(context, expression->AsFunction(), expression, statementValidationScope);
            case Expressions::ExpressionType::Column:
                return CompileColumnExpression(context, expression->AsColumn(), statementValidationScope);
            case Expressions::ExpressionType::Variable:
                return CompileVariableExpression(context, expression->AsVariable());
            case Expressions::ExpressionType::Constant:
                return CompileConstantExpression(expression->AsConstant());
            case Expressions::ExpressionType::Json:
                return CompileJsonExpression(context, expression->AsJson(), statementValidationScope);
            case Expressions::ExpressionType::Expression:
            default:
                break;
        }

        return Errors::ValidationStatus::Error(
    Messages::UNKNOWN_OPERATION,
            context.GetAllocator()
        );
    }

    Errors::ValidationStatus CompileBinaryExpression(
        QueryContext& context,
        Expressions::BinaryExpression *binaryExpr,
        Expressions::Expression *&expression
    ) {
        auto result =
            CompileExpression(context, binaryExpr->left)
            && CompileExpression(context, binaryExpr->right);

        if (!result.IsOk()) return result;

        //validate binary expression action
        if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(binaryExpr->left),
                    Expressions::GetExpressionReturnType(binaryExpr->right)
                )
            );
        }

        if (!binaryExpr->ValidateOperation()) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_OPERATION_ON_DATATYPES(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(binaryExpr->left),
                    Expressions::GetExpressionReturnType(binaryExpr->right)
                )
            );
        }

        FoldExpression(context, binaryExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileBinaryExpression(
        QueryContext& context,
        Expressions::BinaryExpression* binaryExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    ) {
        auto result =
            CompileExpression(context, statementValidationScope, binaryExpr->left)
            && CompileExpression(context, statementValidationScope, binaryExpr->right);

        if (!result.IsOk()) return result;

        if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(binaryExpr->left),
                    Expressions::GetExpressionReturnType(binaryExpr->right)
                )
            );
        }

        if (!binaryExpr->ValidateOperation()) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_OPERATION_ON_DATATYPES(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(binaryExpr->left),
                    Expressions::GetExpressionReturnType(binaryExpr->right)
                )
            );
        }

        FoldExpression(context, binaryExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileLogicalExpression(
        QueryContext &context,
        Expressions::LogicalExpression *logicalExpr,
        Expressions::Expression *&expression
    ) {
        auto result =
            CompileExpression(context, logicalExpr->left)
            && CompileExpression(context, logicalExpr->right);

        if (!result.IsOk()) return result;

        if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->left))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->left));
        if (ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->right));

        FoldExpression(context, logicalExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileLogicalExpression(
        QueryContext& context,
        Expressions::LogicalExpression* logicalExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    ) {
        auto result =
            CompileExpression(context, statementValidationScope, logicalExpr->left)
            && CompileExpression(context, statementValidationScope, logicalExpr->right);

        if (!result.IsOk())return result;

        if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->left))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->left));
        if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->right));

        FoldExpression(context, logicalExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileFunctionExpression(
        QueryContext &context,
        const Expressions::FunctionExpression *funcExpr,
        Expressions::Expression *&expression
    ) {
        for (auto* childExpr : funcExpr->arguments) {
            auto childExpressionResult = CompileExpression(context, childExpr);
            if (!childExpressionResult.IsOk()) return childExpressionResult;
        }

        //validate functionExpression
        DataTypes::String errorMessage(context.GetAllocator());
        if (!funcExpr->ValidateNumberOfArguments(errorMessage))
            return Errors::ValidationStatus::Error(std::move(errorMessage));

        FoldExpression(context, funcExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileFunctionExpression(
        QueryContext &context,
        const Expressions::FunctionExpression *funcExpr,
        Expressions::Expression *&expression,
        StatementValidationScope& statementValidationScope
    ) {
        //validate children expressions and assign return types and ids to column expressions
        for (auto* childExpr : funcExpr->arguments) {
            auto childExpressionResult = CompileExpression(context, statementValidationScope, childExpr);
            if (!childExpressionResult.IsOk()) return childExpressionResult;
        }

        //validate number of arguments
        DataTypes::String errorMessage(context.GetAllocator());
        if (!funcExpr->ValidateNumberOfArguments(errorMessage))
            return Errors::ValidationStatus::Error(std::move(errorMessage));

        FoldExpression(context, funcExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileBranchExpression(
        QueryContext &context,
        Expressions::BranchExpression *branchExpr,
        Expressions::Expression *&expression,
        StatementValidationScope& statementValidationScope
    ) {
        if (!branchExpr->ValidateNumberOfArguments())
            return Errors::ValidationStatus::Error(
                    Messages::INVALID_NUMBER_OF_ARGUMENTS_ON_BRANCH_EXPRESSION,
                    context.GetAllocator()
            );

        for (auto& branch : branchExpr->branches) {
            auto result = CompileExpression(context, statementValidationScope, branch);
            if (!result.IsOk()) return result;

            if (!ValidateExpressionCoercionTypes(DataType::Bool, branch)) {
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_EXPRESSION_TYPE_FOR_BRANCH_EXPRESSION(
                        context.GetAllocator(),
                        Expressions::GetExpressionReturnType(branch)
                    )
                );
            }
        }

        for (auto& resultExpr : branchExpr->results) {
            auto result = CompileExpression(context, statementValidationScope, resultExpr);
            if (!result.IsOk()) return result;
        }

        if (branchExpr->HasBaseCase()) {
            auto result = CompileExpression(context, statementValidationScope, branchExpr->baseCase);
            if (!result.IsOk()) return result;
        }

        const auto returnType = branchExpr->GetReturnType();

        for (const auto& resultExpr : branchExpr->results) {
            if (!ValidateExpressionCoercionTypes(returnType, resultExpr))
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                    context.GetAllocator()
                );
        }

        if (branchExpr->HasBaseCase()) {
            if (!ValidateExpressionCoercionTypes(returnType, branchExpr->baseCase))
                return Errors::ValidationStatus::Error(
                Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                context.GetAllocator()
                );
        }

        FoldExpression(context, branchExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileBranchExpression(
        QueryContext &context,
        Expressions::BranchExpression *branchExpr,
        Expressions::Expression *&expression
    ){
        if (!branchExpr->ValidateNumberOfArguments())
            return Errors::ValidationStatus::Error(
                Messages::INVALID_EXPRESSION_TYPE_FOR_BRANCH_EXPRESSION(
                    context.GetAllocator(),
                    branchExpr->GetReturnType()
                )
            );

        for (auto& branch : branchExpr->branches) {
            auto result = CompileExpression(context, branch);
            if (!result.IsOk()) return result;
        }

        for (auto& resultExpr : branchExpr->results) {
            auto result = CompileExpression(context, resultExpr);
            if (!result.IsOk()) return result;
        }

        if (branchExpr->HasBaseCase()) {
            auto result = CompileExpression(context, branchExpr->baseCase);
            if (!result.IsOk()) return result;
        }

        const auto returnType = branchExpr->GetReturnType();

        for (const auto& resultExpr : branchExpr->results) {
            if (!ValidateExpressionCoercionTypes(returnType, resultExpr))
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                    context.GetAllocator()
                );
        }

        if (branchExpr->HasBaseCase()) {
            if (!ValidateExpressionCoercionTypes(returnType, branchExpr->baseCase))
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                    context.GetAllocator()
                );
        }

        FoldExpression(context, branchExpr, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnExpression(
        const QueryContext& context,
        const Expressions::ColumnExpression *columnExpr
    ){
        return Errors::ValidationStatus::Error(
        Messages::INVALID_COLUMN_ALIAS(
                    context.GetAllocator(),
                    columnExpr->alias
                )
        );
    }

    Errors::ValidationStatus CompileColumnExpression(
        const QueryContext& context,
        Expressions::ColumnExpression* column,
        const StatementValidationScope& statementValidationScope
    ){
        //if wildcard ensure statement is of select statement type
        if (column->alias.ToView() == WILDCARD) {

            auto* selectStatement = dynamic_cast<SelectStatement*>(statementValidationScope.statement);

            if (selectStatement == nullptr)
                return Errors::ValidationStatus::Error(
                    Messages::WILDCARD_USED_ON_NON_SELECT,
                    context.GetAllocator()
                );

            return CompileWildcard(context, column, statementValidationScope, selectStatement);
        }
        return column->HasTableAlias()
            ? CompileColumnWhenTableAliasExists(context, column, statementValidationScope)
            : CompileColumnWhenNoTableAliasExists(context, column, statementValidationScope);
    }

    Errors::ValidationStatus CompileVariableExpression(
        const QueryContext &context,
        Expressions::VariableExpression* variableExpr
    ){
        DataType type;
        if (!context._scope.variables.TryGetValue(variableExpr->normalizedName, type)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_VARIABLE(
                    context.GetAllocator(),
                    variableExpr->name
                )
            );
        }

        variableExpr->dataType = type;
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnExpression(
        const QueryContext &context,
        ColumnName &column,
        StatementValidationScope& statementValidationScope
    ){
        if (!column.alias.Empty()) {
            table_id_t tableId;

            if (!statementValidationScope.tableAliasesDictionary->TryGetValue(column.alias, tableId)) {
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_TABLE_ALIAS(
                           context.GetAllocator(),
                           column.alias
                    )
                );
            }

            column.tableId = tableId;
        }

        bool columnExistsOnTable = false;
        Headers::ColumnHeader columnHeader;
        for (const auto& [key, columns]: *statementValidationScope.tablesColumnsDictionary) {
            const auto columnNameToLower = column.name.ToLower();
            if (!columns.TryGetValue(columnNameToLower, columnHeader))
                continue;

            if (!columnExistsOnTable) {
                columnExistsOnTable = true;

                column.tableId = key;
                column.columnId = columnHeader.id;
                column.index = columnHeader.ordinalPosition;
                column.returnType = static_cast<DataType>(columnHeader.dataType);
            }
        }

        if (!columnExistsOnTable) {
            return Errors::ValidationStatus::Error(
        Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                    context.GetAllocator(),
                    column.name
                )
            );
        }

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileConstantExpression(Expressions::ConstantExpression* constantExpr){
        DataTypes::Coercions::DeduceIntegerType(constantExpr->value);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* jsonExpr,
        const StatementValidationScope& statementValidationScope
    ){
        auto result = CompileColumnExpression(context, jsonExpr->columnPtr, statementValidationScope);
        if (!result.IsOk()) return result;

        if (jsonExpr->pathSegments.Empty())
            return Errors::ValidationStatus::Error(
                Messages::EMPTY_JSON_PATH,
                context.GetAllocator()
            );

        for (auto i = 0; i < jsonExpr->pathSegments.Size(); i++){
            const auto& segment = jsonExpr->pathSegments[i];
            if (segment._accessorType == DataTypes::JsonAccessorType::Scalar
                && i != jsonExpr->pathSegments.Size() - 1
            ) return Errors::ValidationStatus::Error(
            Messages::INVALID_JSON_ACCESSOR_TYPE,
                    context.GetAllocator()
                );
        }

        const auto& lastPathSegment = jsonExpr->pathSegments.Back();
        if (lastPathSegment->_accessorType != DataTypes::JsonAccessorType::Scalar)
            return Errors::ValidationStatus::Error(
                Messages::INVALID_JSON_PATH(
                    context.GetAllocator(),
                    lastPathSegment->_key.ToView()
                )
            );

        //verify json validity maybe

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* jsonExpr
    ){
        auto result = CompileColumnExpression(context, jsonExpr->columnPtr);
        if (!result.IsOk()) return result;

        if (jsonExpr->pathSegments.Empty())
            return Errors::ValidationStatus::Error(
                Messages::EMPTY_JSON_PATH,
                context.GetAllocator()
            );

        for (auto i = 0; i < jsonExpr->pathSegments.Size(); i++){
            const auto& segment = jsonExpr->pathSegments[i];
            if (segment._accessorType == DataTypes::JsonAccessorType::Scalar
                && i != jsonExpr->pathSegments.Size() - 1
                ) return Errors::ValidationStatus::Error(
                Messages::INVALID_JSON_ACCESSOR_TYPE,
                        context.GetAllocator()
                    );
        }

        const auto& lastPathSegment = jsonExpr->pathSegments.Back();
        if (lastPathSegment->_accessorType != DataTypes::JsonAccessorType::Scalar)
            return Errors::ValidationStatus::Error(
                Messages::INVALID_JSON_PATH(
                    context.GetAllocator(),
                    lastPathSegment->_key.ToView()
                )
            );

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnWhenTableAliasExists(
        const QueryContext& context,
        Expressions::ColumnExpression *column,
        const StatementValidationScope& statementValidationScope
    ){
        table_id_t tableId;
        Headers::ColumnHeader columnHeader;
        if (!statementValidationScope.tableAliasesDictionary->TryGetValue(column->tableAlias, tableId)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                            context.GetAllocator(),
                            column->tableAlias
                )
            );
        }

        column->tableId = tableId;

        const auto& columns = statementValidationScope.tablesColumnsDictionary->Get(column->tableId);

        const auto columnAliasToLower = column->alias.ToLower();
        if (!columns.TryGetValue(columnAliasToLower, columnHeader)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_COLUMN_NAME(
                        context.GetAllocator(),
                        column->alias
                )
            );
        }

        column->columnId = columnHeader.id;
        column->returnType = static_cast<DataType>(columnHeader.dataType);
        column->columnIndex = columnHeader.ordinalPosition;
        if (column->name.Empty())
            column->name = columnHeader.name;

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnWhenNoTableAliasExists(
        const QueryContext& context,
        Expressions::ColumnExpression *column,
        const StatementValidationScope& statementValidationScope
    ){
        Headers::ColumnHeader columnHeader;
        bool columnExistsOnStatement = false;

        for (const auto &columns : *statementValidationScope.tablesColumnsDictionary | std::views::values) {
            const auto columnAliasToLower = column->alias.ToLower();
            if (!columns.TryGetValue(columnAliasToLower, columnHeader))
                continue;

            if (columnExistsOnStatement) {
                return Errors::ValidationStatus::Error(
                Messages::AMBIGUOUS_COLUMN_NAME(
                            context.GetAllocator(),
                            column->alias
                        )
                );
            }

            columnExistsOnStatement = true;

            column->tableId = columnHeader.tableId;
            column->columnId = columnHeader.id;
            column->returnType = static_cast<DataType>(columnHeader.dataType);
            column->columnIndex = columnHeader.ordinalPosition;
        }

        if (!columnExistsOnStatement) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_COLUMN_NAME(
                        context.GetAllocator(),
                        column->alias
                )
            );
        }

        if (column->name.Empty())
            column->name = columnHeader.name;

        return Errors::ValidationStatus::Ok();
    }

    bool ValidateExpressionCoercionTypes(const Expressions::Expression *left, const Expressions::Expression *right){
        // If one side is a column expression, its type takes precedence
        const auto leftType = Expressions::GetExpressionReturnType(left);
        const auto rightType = Expressions::GetExpressionReturnType(right);

        const auto isLeftColumn = left->IsColumn();
        const auto isRightColumn = right->IsColumn();

        if (isLeftColumn && right->IsConstant()) {
            const auto* constant = right->AsConstant();
            return
                constant->value.IsNull() ||
                DataTypes::Coercions::IsCoercionAllowed(
                    rightType,
                    leftType
                ) ||
                DataTypes::Coercions::CanBeParsedToType(
                    leftType,
                    constant->value
                );
        }

        if (isRightColumn && left->IsConstant()) {
            const auto* constant = left->AsConstant();

            return
                constant->value.IsNull() ||
                DataTypes::Coercions::IsCoercionAllowed(
                    leftType,
                    rightType
                ) ||
                DataTypes::Coercions::CanBeParsedToType(
                    rightType,
                    constant->value
                );
        }

        return
            DataTypes::Coercions::IsCoercionAllowed(leftType, rightType)
            || DataTypes::Coercions::IsCoercionAllowed(rightType, leftType);
    }

    bool ValidateExpressionCoercionTypes(const DataType type, const Expressions::Expression *expression) {
        if (expression->IsConstant()) {
            auto* constantExpr = expression->AsConstant();

            if (constantExpr->value.IsNull()
                || DataTypes::Coercions::CanBeParsedToType(type, constantExpr->value)
            ) return true;

            return DataTypes::Coercions::IsCoercionAllowed(constantExpr->GetReturnType(), type);
        }

        return DataTypes::Coercions::IsCoercionAllowed(Expressions::GetExpressionReturnType(expression), type);
    }

    Errors::ValidationStatus CompileWildcard(
        const QueryContext& context,
        const Expressions::ColumnExpression* column,
        const StatementValidationScope& statementValidationScope,
        SelectStatement* statement
    ){
        if (column->alias.ToView() != WILDCARD)
            return Errors::ValidationStatus::Ok();

        if (statementValidationScope.indexPos == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::UNEXPECTED_ERROR,
                context.GetAllocator()
            );

        //if no alias is specified get all the columns from the existing tables in the query
        if (column->tableAlias.Empty()) {
            statement->results.erase(statement->results.begin() + *statementValidationScope.indexPos);

            for (const auto &columnsDict : statement->tableColumnsDictionary | std::views::values) {
                AssignColumnsFromWildCardExpression(
                    context,
                    columnsDict,
                    column->tableAlias,
                    statementValidationScope,
                    statement->results
                );
                *statementValidationScope.indexPos += static_cast<int>(columnsDict.size());
            }

            return Errors::ValidationStatus::Ok();
        }

        //else get only from the specified
        table_id_t tableId = 0;
        if (
            !column->tableAlias.Empty()
            && !statementValidationScope.tableAliasesDictionary->TryGetValue(column->tableAlias, tableId)
        ) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                    context.GetAllocator(),
                    column->tableAlias
                )
            );
        }

        //remove the wildcard
        statement->results.erase(statement->results.begin() + *statementValidationScope.indexPos);
        AssignColumnsFromWildCardExpression(
            context,
            statement->tableColumnsDictionary.Get(tableId),
            column->tableAlias,
            statementValidationScope,
            statement->results
        );

        return Errors::ValidationStatus::Ok();
    }

    void AssignColumnsFromWildCardExpression(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader> &columnsDict,
        const DataTypes::String& tableAlias,
        const StatementValidationScope& statementValidationScope,
        DataStructures::PolymorphicArray<Expressions::Expression*>& results
    ) {
        results.Insert(nullptr, *statementValidationScope.indexPos, columnsDict.size());
        // results.resize(results.size() + columnsDict.size());
        for (const auto &header: columnsDict | std::views::values) {
            auto* columnExpression = context._compileContext.Allocate<Expressions::ColumnExpression>(
                header.name,
                tableAlias
            );

            columnExpression->name = header.name;
            columnExpression->columnId = header.id;
            columnExpression->tableId = header.tableId;
            columnExpression->columnIndex = header.ordinalPosition;

            const auto insertPos = *statementValidationScope.indexPos + header.ordinalPosition;

            results[insertPos] = columnExpression;
        }
    }

    void FoldExpression(const QueryContext& context, Expressions::Expression *&expression) {
        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                FoldExpression(context, expression->AsBinary(), expression);
                break;
            case Expressions::ExpressionType::Logical:
                FoldExpression(context, expression->AsLogical(), expression);
                break;
            case Expressions::ExpressionType::Branch:
                FoldExpression(context, expression->AsBranch(), expression);
                break;
            case Expressions::ExpressionType::Function:
                FoldExpression(context, expression->AsFunction(), expression);
                break;
            case Expressions::ExpressionType::Expression:
            case Expressions::ExpressionType::Column:
            case Expressions::ExpressionType::Constant:
            case Expressions::ExpressionType::Variable:
            case Expressions::ExpressionType::Json:
                break;
        }
    }

    void FoldExpression(
        const QueryContext& context,
        const Expressions::BinaryExpression *castExpr,
        Expressions::Expression *&expression
    ){
        if (
            !castExpr->left->IsConstant()
            || !castExpr->right->IsConstant()
        ) return;

        EvaluateExpression(context, expression);
    }

    void FoldExpression(
        const QueryContext& context,
        Expressions::LogicalExpression *castExpr,
        Expressions::Expression *&expression
    ){
        //If expression is of type OR and either right or left is a constant, it will always be true
        if (castExpr->IsOr()) {
            if (castExpr->left->IsConstant()) {
                PropagateExpression(expression, castExpr->left);
                return;
            }

            if (castExpr->right->IsConstant())
                PropagateExpression(expression, castExpr->right);

            return;
        }

        //if expression is and and it has one constant propagate child
        //keep the right
        if (castExpr->left->IsConstant()) {
            TryPropagateChildExpression(expression, castExpr->left, castExpr->right);
            return;
        }

        if (castExpr->right->IsConstant())
            TryPropagateChildExpression(expression, castExpr->right, castExpr->left);
    }

    void FoldExpression(
        const QueryContext& context,
        const Expressions::FunctionExpression *castExpr,
        Expressions::Expression *&expression
    ){
        for (const auto& argument : castExpr->arguments) {
            if (!argument->IsConstant())
                return;
        }
        EvaluateExpression(context, expression);
    }

    void FoldExpression(
        const QueryContext& context,
        Expressions::BranchExpression *castExpr,
        Expressions::Expression *&expression
    ){
        for (int i = 0;i < castExpr->branches.Size(); i++) {
            const auto* branch = castExpr->branches[i];

            if (!branch->IsConstant()) return;

            const auto value = branch->AsConstant()->Evaluate(Expressions::EvaluationContext(context.GetAllocator()));
            if (value.AsBool()) {
                PropagateExpression(expression, castExpr->results[i]);
                FoldExpression(context, expression);
                return;
            }
        }
    }

    void PropagateExpression(Expressions::Expression *&expression, Expressions::Expression *&childExpr) {
        auto* expr = childExpr;
        childExpr = nullptr;
        expression = expr;
    }

    void EvaluateExpression(const QueryContext& context, Expressions::Expression *&expression) {
        auto value = Expressions::EvaluateExpression(expression, Expressions::EvaluationContext(context.GetAllocator()));
        expression = context._compileContext.Allocate<Expressions::ConstantExpression>(value);
    }

    void AssignConstantToExpression(const QueryContext& context, Expressions::Expression *&expression) {
        auto value = Value(true, context._compileContext.GetAllocator(), 0);
        expression = context._compileContext.Allocate<Expressions::ConstantExpression>(value);
    }

    void AssignColumnIndicesToExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        Expressions::Expression *expression
    ){
        if (expression == nullptr) return;

        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                AssignColumnIndicesToBinaryExpression(columnIndicesDictionary, expression->AsBinary());
                break;
            case Expressions::ExpressionType::Logical:
                AssignColumnIndicesToLogicalExpression(columnIndicesDictionary, expression->AsLogical());
                break;
            case Expressions::ExpressionType::Branch:
                AssignColumnIndicesToBranchExpression(columnIndicesDictionary, expression->AsBranch());
                break;
            case Expressions::ExpressionType::Function:
                AssignColumnIndicesToFunctionExpression(columnIndicesDictionary, expression->AsFunction());
                break;
            case Expressions::ExpressionType::Column:
                AssignColumnIndicesToColumnExpression(columnIndicesDictionary, expression->AsColumn());
                break;
            case Expressions::ExpressionType::Json:
                AssignColumnIndicesToColumnExpression(columnIndicesDictionary, expression->AsJson()->columnPtr);
                break;
            case Expressions::ExpressionType::Variable:
            case Expressions::ExpressionType::Expression:
            case Expressions::ExpressionType::Constant:
            default:
                break;
        }
    }

    void AssignColumnIndicesToBinaryExpression(
        const Dictionary<Int, column_index_t> &columnIndicesDictionary,
        const Expressions::BinaryExpression *expression
    ) {
        AssignColumnIndicesToExpression(columnIndicesDictionary, expression->left);
        AssignColumnIndicesToExpression(columnIndicesDictionary, expression->right);
    }

    void AssignColumnIndicesToLogicalExpression(
        const Dictionary<Int, column_index_t> &columnIndicesDictionary,
        const Expressions::LogicalExpression *expression
    ){
        AssignColumnIndicesToExpression(columnIndicesDictionary, expression->left);
        AssignColumnIndicesToExpression(columnIndicesDictionary, expression->right);
    }

    void AssignColumnIndicesToBranchExpression(
    const Dictionary<Int, column_index_t> &columnIndicesDictionary,
    const Expressions::BranchExpression *expression
    ) {
        for (const auto& argument : expression->arguments)
            AssignColumnIndicesToExpression(columnIndicesDictionary, argument);

        for (const auto& branch : expression->branches)
            AssignColumnIndicesToExpression(columnIndicesDictionary, branch);

        for (const auto& result : expression->results)
            AssignColumnIndicesToExpression(columnIndicesDictionary, result);

        if (expression->HasBaseCase())
            AssignColumnIndicesToExpression(columnIndicesDictionary, expression->baseCase);
    }

    void AssignColumnIndicesToFunctionExpression(
        const Dictionary<Int, column_index_t> &columnIndicesDictionary,
        const Expressions::FunctionExpression *expression
    ) {
        for (auto* childExpr : expression->arguments)
            AssignColumnIndicesToExpression(columnIndicesDictionary, childExpr);
    }

    void AssignColumnIndicesToColumnExpression(
        const Dictionary<Int, column_index_t> &columnIndicesDictionary,
        Expressions::ColumnExpression *expression
    ) {
        expression->columnIndex = columnIndicesDictionary.Get(expression->columnId);
    }

    Errors::ValidationStatus CompilePostProjectionExpression(
        const QueryContext& context,
        Expressions::Expression *expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    ){
        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                return CompilePostProjectionBinaryExpression(context, expression->AsBinary(), postProjectionAliases);
            case Expressions::ExpressionType::Logical:
                return CompilePostProjectionLogicalExpression(context, expression->AsLogical(), postProjectionAliases);
            case Expressions::ExpressionType::Branch:
                return CompilePostProjectionBranchExpression(context, expression->AsBranch(), postProjectionAliases);
            case Expressions::ExpressionType::Function:
                return CompilePostProjectionFunctionExpression(context, expression->AsFunction(), postProjectionAliases);
            case Expressions::ExpressionType::Column:
                return CompilePostProjectionColumnExpression(context, expression->AsColumn(), postProjectionAliases);
            case Expressions::ExpressionType::Variable:
            case Expressions::ExpressionType::Constant:
            case Expressions::ExpressionType::Expression:
            default:
                break;
        }

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompilePostProjectionColumnExpression(
        const QueryContext& context,
        Expressions::ColumnExpression *column,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    ){
        const Expressions::Expression* expression;
        if (!postProjectionAliases.TryGetValue(column->alias, expression)) {
            return Errors::ValidationStatus::Error(
            Messages::INVALID_COLUMN_NAME(
                        context.GetAllocator(),
                        column->alias
                )
            );
        }

        column->returnType = Expressions::GetExpressionReturnType(expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompilePostProjectionBinaryExpression(
        const QueryContext& context,
        const Expressions::BinaryExpression *expression,
        const Dictionary<DataTypes::String, const Expressions::Expression *> &postProjectionAliases
    ){
        return
            CompilePostProjectionExpression(context, expression->left, postProjectionAliases)
            && CompilePostProjectionExpression(context, expression->right, postProjectionAliases);
    }

    Errors::ValidationStatus CompilePostProjectionLogicalExpression(
        const QueryContext& context,
        const Expressions::LogicalExpression *expression,
        const Dictionary<DataTypes::String, const Expressions::Expression *> &postProjectionAliases
    ){
        return
            CompilePostProjectionExpression(context, expression->left, postProjectionAliases)
            && CompilePostProjectionExpression(context, expression->right, postProjectionAliases);
    }

    Errors::ValidationStatus CompilePostProjectionFunctionExpression(
        const QueryContext& context,
        const Expressions::FunctionExpression *expression,
        const Dictionary<DataTypes::String, const Expressions::Expression *> &postProjectionAliases
    ){
        for (auto* childExpr : expression->arguments) {
            auto childExprStatus = CompilePostProjectionExpression(context, childExpr, postProjectionAliases);
            if (!childExprStatus.IsOk()) return childExprStatus;
        }
        //validate number of arguments
        DataTypes::String errorMessage;
        if (!expression->ValidateNumberOfArguments(errorMessage))
            return Errors::ValidationStatus::Error(std::move(errorMessage));

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompilePostProjectionBranchExpression(
        const QueryContext& context,
        const Expressions::BranchExpression *expression,
        const Dictionary<DataTypes::String, const Expressions::Expression *> &postProjectionAliases
    ){
        for (const auto& argument : expression->arguments) {
            auto result = CompilePostProjectionExpression(context, argument, postProjectionAliases);

            if (!result.IsOk()) return result;
        }

        for (const auto& branch : expression->branches) {
            auto result = CompilePostProjectionExpression(context, branch, postProjectionAliases);

            if (!result.IsOk()) return result;
        }

        for (const auto& resultExpr : expression->results) {
            auto result = CompilePostProjectionExpression(context, resultExpr, postProjectionAliases);

            if (!result.IsOk()) return result;
        }

        return Errors::ValidationStatus::Ok();
    }

    void AssignPostProjectionIndicesToExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        Expressions::Expression *expression
    ){
        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                AssignPostProjectionIndicesToBinaryExpression(columnIndicesDictionary, expression->AsBinary());
                break;
            case Expressions::ExpressionType::Logical:
                AssignPostProjectionIndicesToLogicalExpression(columnIndicesDictionary, expression->AsLogical());
                break;
            case Expressions::ExpressionType::Branch:
                AssignPostProjectionIndicesToBranchExpression(columnIndicesDictionary, expression->AsBranch());
                break;
            case Expressions::ExpressionType::Function:
                AssignPostProjectionIndicesToFunctionExpression(columnIndicesDictionary, expression->AsFunction());
                break;
            case Expressions::ExpressionType::Column:
                AssignPostProjectionIndicesToColumnExpression(columnIndicesDictionary, expression->AsColumn());
                break;
            case Expressions::ExpressionType::Expression:
            case Expressions::ExpressionType::Constant:
            case Expressions::ExpressionType::Variable:
            default:
                break;
        }
    }

    void AssignPostProjectionIndicesToBinaryExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        const Expressions::BinaryExpression *expression
    ){
        AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->left);
        AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->right);
    }

    void AssignPostProjectionIndicesToLogicalExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        const Expressions::LogicalExpression *expression
    ){
        AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->left);
        AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->right);
    }

    void AssignPostProjectionIndicesToFunctionExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        const Expressions::FunctionExpression *expression
    ){
        for (auto* childExpr : expression->arguments)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, childExpr);
    }

    void AssignPostProjectionIndicesToBranchExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        const Expressions::BranchExpression *expression
    ) {
        for (auto* argument : expression->arguments)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, argument);

        for (auto* branch : expression->branches)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, branch);

        for (auto* result : expression->results)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, result);

        if (expression->HasBaseCase())
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->baseCase);
    }

    void AssignPostProjectionIndicesToBranchExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        Expressions::BranchExpression *expression
    ){
        for (auto*& argument : expression->arguments)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, argument);
        for (auto*& branch : expression->branches)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, branch);
        for (auto*& resultExpr : expression->results)
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, resultExpr);
        if (expression->HasBaseCase())
            AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->baseCase);
    }

    void AssignPostProjectionIndicesToColumnExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        Expressions::ColumnExpression *expression
    ) {
        expression->columnIndex = columnIndicesDictionary.Get(expression->alias);
    }

    Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(const QueryContext& context, const DataType type) {
      return Errors::ValidationStatus::Error(
        Messages::CLAUSE_CANNOT_BE_EVALUATED_TO_BOOLEAN(
            context._compileContext.GetAllocator(),
            SqlTypesString[static_cast<Int>(type)]
        )
      );
    }

    void TryPropagateChildExpression(
        Expressions::Expression*& expression,
        Expressions::Expression*& leftExpr,
        Expressions::Expression*& rightExpr
    ){
        const auto* left = leftExpr->AsConstant();
        if (left->value.IsNull()) return;

        if(
            DataTypes::Coercions::CanBeParsedToType(DataType::Bool, left->value)
            && left->value.AsBool() == false
        ) {
            PropagateExpression(expression, leftExpr);
            return;
        }

        PropagateExpression(expression, rightExpr);
    }
}
