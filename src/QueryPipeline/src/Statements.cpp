#include "../include/Statements.h"

#include "../../CoreEngine/include/Database.h"
#include "../../Systemic/include/Coercions/Coercions.h"
#include "../../Systemic/include/Functions/StringFunctions.h"
#include "../../Server/include/Server.h"
#include "../include/LogicalPlan.h"
#include "../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include <ranges>
#include <ValidationMessages.h>

#include "Optimizer.h"
#include "Parser.h"
#include "../../Systemic/include/DataTypes/DataTypes.StaticData.h"
#include "../../CoreEngine/include/DataStorage/SerializedRow.h"

namespace QueryPipeline::Statements {
    Statement::Statement()
        : table(nullptr), databaseId(Constants::SYSTEM_CATALOG_ID), _slotCount(DEFAULT_SLOT_INDEX){}

    Errors::ValidationStatus Statement::CompileBase(const QueryContext& context)const{
        Errors::ValidationStatus validationStatus(context.GetAllocator());

        if (!context._session || !context._session->user || !context._session->user->role){
            validationStatus.code = Errors::ValidationError::Error;
            validationStatus.message = DataTypes::String::FromView(
                Messages::FAILED_TO_FETCH_USER_SESSION,
                context.GetAllocator()
            );
        }

        if (!context._session->user->role->HasPermission(this->RequiredPermissions())) {
            validationStatus.code = Errors::ValidationError::Error;
            validationStatus.message = DataTypes::String::Concat(context.GetAllocator(), "User", context._session->user->name, " is not authorized to perform this action.");
        }

        return validationStatus;
    }

    Errors::ValidationStatus Statement::Compile(QueryContext& context){
        auto result = this->CompileBase(context);
        if (!result.IsOk())
            return result;

        return this->CompileDerived(context);
    }

   DeclareVariableStatement::DeclareVariableStatement()
       : expression(nullptr), type(DataType::Null){}

    Errors::ValidationStatus DeclareVariableStatement::CompileDerived(QueryContext& context) {
        const auto& variableType = this->variable.GetType();

        auto validationStatus = Errors::ValidationStatus(context.GetAllocator());
        if (this->expression) {
            auto res = CompileExpression(context, this->expression);

            if (!res.IsOk()) return res;

            if (variableType != DataType::Null && !ValidateExpressionCoercionTypes(variableType, this->expression)) {
                validationStatus.code = Errors::ValidationError::Error;
                validationStatus.message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
            Expressions::GetExpressionReturnType(this->expression),
            variableType
                );

                return validationStatus;
            }

            if (variableType == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        context._scope.variables.ForceAdd(this->variable.GetNormalizedName(), variableType);
        return validationStatus;
    }

    constexpr Security::Permission DeclareVariableStatement::RequiredPermissions()const {
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan * DeclareVariableStatement::ToLogical(QueryContext& context) {
        return context._compileContext.Allocate<LogicalDeclareVariable>(context._session->sessionId, this->variable, this->expression);
    }

    SetVariableStatement::SetVariableStatement()
        : expression(nullptr), type(DataType::Null){}

    Errors::ValidationStatus SetVariableStatement::CompileDerived(QueryContext& context) {
        const auto datatype = this->variable.GetType();

        auto validationStatus = Errors::ValidationStatus(context.GetAllocator());
        if (this->expression) {
            auto res = CompileExpression(context, this->expression);

            if (!res.IsOk()) return res;

            if (datatype != DataType::Null && !ValidateExpressionCoercionTypes(datatype, this->expression)) {
                validationStatus.code = Errors::ValidationError::Error;
                validationStatus.message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(this->expression),
                    datatype
                );

                return validationStatus;
            }

            if (datatype == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        context._scope.variables.ForceAdd(this->variable.GetNormalizedName(), datatype);
        return validationStatus;
    }

  constexpr Security::Permission SetVariableStatement::RequiredPermissions() const {
    return Constants::DB_WRITER_PERMISSIONS;
  }

  LogicalPlan * SetVariableStatement::ToLogical(QueryContext& context) {
    return context._compileContext.Allocate<LogicalDeclareVariable>(context._session->sessionId, this->variable, this->expression);
  }

  Errors::ValidationStatus CreateUserStatement::CompileDerived(QueryContext& context){
    if (this->username.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_USERNAME, context.GetAllocator());
    if (this->password.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_PASSWORD, context.GetAllocator());
    if (this->role.Empty())
        return Errors::ValidationStatus::Error(Messages::EMPTY_ROLE, context.GetAllocator());
    if (Network::Server::Get().UserExists(this->username))
        return Errors::ValidationStatus::Error(Messages::USER_ALREADY_EXISTS, context.GetAllocator());
    if (!Network::Server::Get().RoleExists(this->role))
        return Errors::ValidationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());

    return Errors::ValidationStatus(context.GetAllocator());
  }

  constexpr Security::Permission CreateUserStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

    LogicalPlan* CreateUserStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalCreateUser>(context._session->sessionId, this->username, this->password, this->role);
    }

    Errors::ValidationStatus GrantRoleStatement::CompileDerived(QueryContext& context){
        if (!Network::Server::Get().UserExists(this->username))
            return Errors::ValidationStatus::Error(Messages::USER_DOES_NOT_EXIST, context.GetAllocator());
        if (!Network::Server::Get().RoleExists(this->role))
            return Errors::ValidationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());
        return Errors::ValidationStatus(context.GetAllocator());
    }

    constexpr Security::Permission GrantRoleStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan * GrantRoleStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalGrantRole>(context._session->sessionId, this->username, this->role);
    }

    Errors::ValidationStatus DeleteStatement::CompileDerived(QueryContext& context){
        auto result = this->table->Compile(context, this->databaseId, this->_slotCount);

        if (!result.IsOk()) return result;

        if (this->where.expression == nullptr) return result;

        const auto columnsDict = CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(
            context.GetAllocator(),
            this->table->_tableId
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

    Errors::ValidationStatus JoinStatement::Compile(QueryContext& context, const Int databaseId){
        this->databaseId = databaseId;
        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::MISSING_TABLE_IN_JOIN,
                context.GetAllocator()
            );

        return this->table->Compile(context, this->databaseId, this->_slotCount);
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
    const Memory::IAllocator* allocator,
    const UnsignedSmallInt numberOfTables
    ):  _tableColumnsArray(allocator, numberOfTables), _indexPos(nullptr), _statement(nullptr){
        _tableColumnsArray.AlignSize();
    }

    StatementValidationScope::StatementValidationScope(
        Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>>& tableColumnsArray,
        Statement *statement,
        int *indexPos
    ):  _tableAliasesDict(std::move(tableAliasesDictionary)),
        _tableColumnsArray(std::move(tableColumnsArray)),
        _indexPos(indexPos), _statement(statement) {}

    StatementValidationScope::StatementValidationScope(
        Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
        Statement* statement,
        int* indexPos
    ):   _tableAliasesDict(std::move(tableAliasesDictionary)),
                _indexPos(indexPos), _statement(statement) {}

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

    DataSource::DataSource(const ::Memory::IAllocator* allocator)
        :   schema(DataTypes::String::FromView(Constants::DEFAULT_SCHEMA_NAME, allocator)),
            _databaseId(INVALID_DATABASE_ID), _tableId(INVALID_TABLE_ID),
            _schemaId(INVALID_SCHEMA_ID), _ordinalPosition(INVALID_ORDINAL_POS), _slotIndex(DEFAULT_SLOT_INDEX){}

    DataTypes::String DataSource::GetAlias(const QueryContext& context) const{
        return  (this->alias.Empty())
        ? this->GetFullName(context)
            : this->alias;
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

    Errors::ValidationStatus DataSource::Compile(
        const QueryContext& context,
        const Int databaseId,
        UnsignedSmallInt& outSlotCount
    ) {
        const auto tableHeader = (!this->database.Empty())
            ? CoreEngine::SystemCatalog::Get().SelectTable(
                context.GetAllocator(),
                DataTypes::StringView::ViewOf(this->database),
                DataTypes::StringView::ViewOf(this->name)
            )
            : CoreEngine::SystemCatalog::Get().SelectTable(
                context.GetAllocator(),
                databaseId,
                DataTypes::StringView::ViewOf(this->name),
                DataTypes::StringView::ViewOf(this->schema)
            );

        if (tableHeader.id == INVALID_TABLE_ID){
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE(context.GetAllocator(), this->GetFullName(context))
            );
        }

        this->_tableId = tableHeader.id;
        this->_ordinalPosition = tableHeader.ordinalPosition;
        this->_databaseId = tableHeader.databaseId;
        this->_slotIndex = outSlotCount++;

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus DataSource::ValidateTableCreate(const QueryContext& context, const Int selectedDatabaseId){
        const auto tableHeader = (!this->database.Empty())
            ? CoreEngine::SystemCatalog::Get().SelectTable(
                context.GetAllocator(),
                DataTypes::StringView::ViewOf(this->database),
                DataTypes::StringView::ViewOf(this->name)
            )
            : CoreEngine::SystemCatalog::Get().SelectTable(
                context.GetAllocator(),
                selectedDatabaseId,
                DataTypes::StringView::ViewOf(this->name),
                DataTypes::StringView::ViewOf(this->schema)
            );

        if (tableHeader.id != INVALID_TABLE_ID)
            return Errors::ValidationStatus::Error(
            Messages::TABLE_ALREADY_EXISTS(context.GetAllocator(), this->GetFullName(context))
            );

        this->_databaseId = selectedDatabaseId;
        return Errors::ValidationStatus::Ok();
    }

    CreateTableStatement::CreateTableStatement(){
        this->table = nullptr;
        this->constraint = nullptr;
    }

    Errors::ValidationStatus CreateTableStatement::CompileSchema(const QueryContext& context) const{
        const auto& schemasDict = CoreEngine::SystemCatalog::Get().SelectSchemasToDictionary(context.GetAllocator(), this->databaseId);
        Headers::SchemaHeader schemaHeader;

        this->table->schema.ToLowerInPlace();
        if (!schemasDict.TryGetValue(this->table->schema, schemaHeader))
            return Errors::ValidationStatus::Error(
                Messages::SCHEMA_DOES_NOT_EXIST(context.GetAllocator(), this->table->schema)
            );

        this->table->_schemaId = schemaHeader.id;
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
        if (!COLUMN_SIZES_BY_TYPENAME.TryGetValue(DataTypes::StringView::ViewOf(column->type.name), columnSize)) {
            return Errors::ValidationStatus::Error(
                Messages::DATATYPE_DOES_NOT_EXIST(context.GetAllocator(), column->type.name)
            );
        }

        if (columnSize != 0)
            column->type.size = columnSize;

        const auto dataType = COLUMN_TYPENAMES_TO_ENUMS.Get(DataTypes::StringView::ViewOf(column->type.name));

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
            context._session->sessionId,
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
        : Statement(), columnHeaders(allocator), _projections(allocator), _joins(allocator) {
        this->top = INVALID_TOP;
        this->distinct = false;
        this->orderBy = nullptr;
    }

    Dictionary<DataTypes::String, column_index_t> SelectStatement::CreatePostProjectionIndicesDictionary() const{
        Dictionary<DataTypes::String, column_index_t> dict;

        for (int i = 0;i < this->_projections.Size(); i++) {
            const auto& resultExpr = this->_projections[i];

            if (resultExpr->name.Empty()) continue;
            dict.Add(resultExpr->name, i);
        }

        return dict;
    }

    bool SelectStatement::HasTopStatement() const{ return this->top != INVALID_TOP; }

    bool SelectStatement::HasJoins()const{ return !this->_joins.Empty(); }

    bool SelectStatement::HasWhere() const{ return this->where.expression != nullptr; }

    bool SelectStatement::IsConstant() const{ return this->table == nullptr; }

    Errors::ValidationStatus SelectStatement::CompileNoTableStatement(QueryContext& context){
        for (auto& resultExpr : this->_projections){
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
        Dictionary<DataTypes::String, table_id_t>& aliasesDict
    ){
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>> tableColumnsArray(
            context.GetAllocator(),
            this->_joins.Size() + 1
        );

        //Add Base Table to the dictionaries
        aliasesDict.Add(this->table->GetAlias(context), this->table->_slotIndex);
        tableColumnsArray.Push(
            CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId)
        );

        //Add all the join tables to the dictionaries
        for (const auto* join: this->_joins) {
            aliasesDict.Add(
                join->table->GetAlias(context),
                join->table->_slotIndex
            );

            tableColumnsArray.Push(
                CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(context.GetAllocator(), join->table->_tableId)
            );
        }

        StatementValidationScope statementValidationScope(
            aliasesDict,
            tableColumnsArray,
            this
        );

        //start resolving aliases
        for (int i = 0; i < this->_projections.Size(); i++) {
            statementValidationScope._indexPos = &i;
            auto expressionResult = CompileExpression(context, statementValidationScope, this->_projections[i]);

            if (!expressionResult.IsOk()) return expressionResult;
        }

        auto result = this->CompileWhereClause(context, statementValidationScope);
        if (!result.IsOk()) return result;

        //validate join expressions
        statementValidationScope._indexPos = nullptr;
        for (const auto& join: this->_joins) {
            auto expressionResult = CompileExpression(context, statementValidationScope, join->expression);

            if (!expressionResult.IsOk()) return expressionResult;
        }

        if (this->orderBy == nullptr)
            return Errors::ValidationStatus::Ok();

        Dictionary<DataTypes::String, const Expressions::Expression*> postProjectionAliases;

        for (const auto* resultExpr : this->_projections) {
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
        auto* current = context._compileContext.Allocate<LogicalTableScan>(table, predicatesResult.PushDownFilter(table->_tableId));
        return context._compileContext.Allocate<LogicalMaterialize>(
            current,
            table->_tableId,
            table->_slotIndex
        );
    }

    LogicalPlan* SelectStatement::BuildJoinsPlan(
        const QueryContext& context,
        const JoinOrderAnalyzeResult& joinReorderResult,
        const PredicatePushDownResult& predicatesResult
    ) const{
        const auto leftTableId = this->table->_tableId;
        auto* current = SelectStatement::BuildTableScanPlan(context, this->table, predicatesResult);

        for (const auto& join : joinReorderResult.orderedJoins){
            auto* right = SelectStatement::BuildTableScanPlan(context, join->table, predicatesResult);

            current = context._compileContext.Allocate<LogicalJoin>(
                current,
                right,
                join->expression,
                join->type,
                leftTableId,
                join->table->_tableId
            );
        }

        return current;
  }

    // Dictionary<Int, column_index_t> SelectStatement::BuildColumnsIndicesDictionary(
    //     const QueryContext& context,
    //     const DataStructures::PolymorphicArray<table_id_t>& joinOrder
    // ) const{
    //     Dictionary<Int, column_index_t> result;
    //     column_index_t columnIndex = 0;
    //
    //     for (const auto& tableId: joinOrder) {
    //     //TODO cache them at the beginning
    //         const auto& columns = CoreEngine::SystemCatalog::Get().SelectColumns(context.GetAllocator(), tableId);
    //         for (const auto &column : columns) {
    //             if (result.Contains(column.id)) continue;
    //             result.Add(column.id, columnIndex + column.ordinalPosition);
    //         }
    //
    //         columnIndex += columns.Size();
    //     }
    //
    //     return result;
    // }

    // void SelectStatement::AssignColumnsToIndices(
    //     const QueryContext& context,
    //     const DataStructures::PolymorphicArray<table_id_t>& order
    // )const {
    //     const auto columnIndicesDictionary = this->BuildColumnsIndicesDictionary(context, order);
    //     for (const auto& resultExpr : this->_projections)
    //         AssignColumnIndicesToExpression(columnIndicesDictionary, resultExpr);
    //     if (this->where.expression != nullptr)
    //         AssignColumnIndicesToExpression(columnIndicesDictionary, this->where.expression);
    //     for (const auto* join : this->_joins)
    //         AssignColumnIndicesToExpression(columnIndicesDictionary, join->expression);
    // }

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
        if (!this->_joins.Empty() && this->table == nullptr) {
            return Errors::ValidationStatus::Error(
             Messages::JOIN_WITH_NO_BASE_TABLE_SELECT,
             context.GetAllocator()
            );
        }

        //resolve expressions here since no column is to be used
        if (this->table == nullptr) return this->CompileNoTableStatement(context);

        auto tableResult = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableResult.IsOk())
            return tableResult;

        for (auto* join: this->_joins) {
            auto joinResult = join->Compile(context, this->databaseId);
            if (!joinResult.IsOk())
                return joinResult;
        }

        Dictionary<DataTypes::String, UnsignedSmallInt> aliasesDict;
        return this->Compile(context, aliasesDict);
    }

    constexpr Security::Permission SelectStatement::RequiredPermissions() const{
        return Constants::DB_READER_PERMISSIONS;
    }

    LogicalPlan* SelectStatement::ToLogical(QueryContext& context){
        if (this->IsConstant())
            return context._compileContext.Allocate<LogicalProject>(nullptr, this->_projections, this->_slotCount);

        const Optimizer optimizer(context);
        const auto joinReorderResult = optimizer.DetermineJoinOrder(this);

        auto predicatesResult = optimizer.PushDownPredicates(
            joinReorderResult.order,
            this->where.expression,
            joinReorderResult.orderedJoins
        );

        auto* current = this->BuildJoinsPlan(context, joinReorderResult, predicatesResult);

        if (predicatesResult.remainingPredicate != nullptr)
            current = context._compileContext.Allocate<LogicalFilter>(current, predicatesResult.remainingPredicate, this->_slotCount);

        const auto postProjectionIndicesDictionary = this->CreatePostProjectionIndicesDictionary();

        current = context._compileContext.Allocate<LogicalProject>(current, this->_projections, this->_slotCount);

        this->BuildOrderByStatement(current, postProjectionIndicesDictionary);

        if (this->distinct)
            current = context._compileContext.Allocate<LogicalDistinct>(current);

        if (this->HasTopStatement())
            current = context._compileContext.Allocate<LogicalTop>(current, this->top);

        return current;
    }

    Errors::ValidationStatus CreateDbStatement::CompileDerived(QueryContext& context){
        if (CoreEngine::SystemCatalog::Get().DatabaseExists(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name))) {
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
        return context._compileContext.Allocate<LogicalCreateDatabase>(context._session->sessionId, this->name);
    }

    Errors::ValidationStatus DropDbStatement::CompileDerived(QueryContext& context){
        const auto database = CoreEngine::SystemCatalog::Get().SelectDatabase(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name));

        if (database.name.Empty()) {
            return Errors::ValidationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_DROP(
                    context.GetAllocator(),
                    DataTypes::StringView::ViewOf(this->name)
                )
            );
        }

        if (database.isSystem) {
            return Errors::ValidationStatus::Error(
                Messages::CANNOT_DROP_SYSTEM_DATABASE(
                    context.GetAllocator(),
                    DataTypes::StringView::ViewOf(this->name)
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
        const auto dbHeader = CoreEngine::SystemCatalog::Get().SelectDatabase(
            context.GetAllocator(),
            DataTypes::StringView::ViewOf(this->name)
        );

        if (dbHeader.id == INVALID_DATABASE_ID) {
            return Errors::ValidationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_USE(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name))
            );
        }

        this->databaseId = dbHeader.id;
        return Errors::ValidationStatus::Ok();
    }

    constexpr Security::Permission UseDatabaseStatement::RequiredPermissions() const{
        return Constants::GUEST_PERMISSIONS;
    }

    LogicalPlan * UseDatabaseStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalUseDatabase>(context._session->sessionId, this->databaseId);
    }

    Int InsertStatement::InsertDefaultValue(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<Expressions::Expression*>& defaultExpressions,
        const Headers::ColumnHeader &header,
        Headers::DefaultValuesHeader& defaultValue
    ){
        auto value = Value::FromExternalStorage(
            reinterpret_cast<object_t*>(defaultValue.value.Data()),
            defaultValue.value.Size(),
            static_cast<DataType>(header.dataType),
            allocator,
            static_cast<column_index_t>(header.ordinalPosition)
        );

        auto* constantExpression = allocator->Allocate<Expressions::ConstantExpression>(value);
        defaultExpressions.Push(constantExpression);
        return defaultExpressions.Size();
    }

    void InsertStatement::InsertNullValues(const QueryContext& context, const Headers::ColumnHeader& header){
        for (auto& [insertColumns] : this->values){
            auto* expression = context._compileContext.Allocate<Expressions::ConstantExpression>(Value::Null(header.ordinalPosition));
            insertColumns.Push(expression);
        }
    }

    Errors::ValidationStatus InsertStatement::ValidateReturnType(
        const QueryContext& context,
        StatementValidationScope& validationScope,
        Expressions::Expression*& expression,
        const DataTypes::String& columnName
    ) const{
        const auto valueType = Expressions::GetExpressionReturnType(expression);

        const auto& columnsDict = validationScope._tableColumnsArray[this->table->_slotIndex];

        const auto columnNameToLower = columnName.ToLower();
        const auto& columnHeader = columnsDict.Get(columnNameToLower);

        const auto columnType = static_cast<DataType>(columnHeader.dataType);

        if (valueType == columnType)
            return Errors::ValidationStatus::Ok();

        if (DataTypes::Coercions::IsCoercionAllowed(
            valueType,
            columnType
        )){
            InsertCastExpression(context, expression, columnType);
            FoldCastExpression(context, expression);
            return Errors::ValidationStatus::Ok();
        }

        if (expression->IsConstant()) {
            const auto* constantExpr = expression->AsConstant();

            if (constantExpr->value.IsNull()) {
                if (columnHeader.isNullable)
                    return Errors::ValidationStatus::Ok();

                return Errors::ValidationStatus::Error(
            Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                        context.GetAllocator(),
                        columnHeader.name
                    )
                );
            }

            if (DataTypes::Coercions::CanBeParsedToType(columnType, constantExpr->value)){
                InsertCastExpression(context, expression, columnType);
                EvaluateExpression(context, expression);
                return Errors::ValidationStatus::Ok();
            }
        }

        return Errors::ValidationStatus::Error(
            Messages::CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
                context.GetAllocator(),
                columnName,
                SQL_TYPES_NAMES[static_cast<Int>(columnType)],
                SQL_TYPES_NAMES[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::ValidationStatus InsertStatement::ValidateSelectStatement(QueryContext& context, StatementValidationScope& validationScope)const{
        if (this->selectStatement == nullptr)
            return Errors::ValidationStatus::Ok();

        if (this->selectStatement->_projections.Size() != this->columns.Size())
            return Errors::ValidationStatus::Error(
                Messages::INSERT_STATEMENT_INVALID_NUMBER_OF_ARGUMENTS_ON_SUB_SELECT,
                context.GetAllocator()
            );

        this->selectStatement->databaseId = this->databaseId;

        auto selectStatus = this->selectStatement->CompileDerived(context);
        if (!selectStatus.IsOk())
            return selectStatus;

        for (Int i = 0;i < this->selectStatement->_projections.Size();i++) {
            auto returnTypeStatus = this->ValidateReturnType(
                context,
                validationScope,
                this->selectStatement->_projections[i],
                this->columns[i].name
            );

            if (!returnTypeStatus.IsOk())
                return returnTypeStatus;
        }

        return Errors::ValidationStatus::Ok();
    }

    bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

    Errors::ValidationStatus InsertStatement::ResolveAliases(QueryContext& context, StatementValidationScope& validationScope){
        validationScope._tableAliasesDict.Add(this->table->GetAlias(context), this->table->_tableId);

        for (auto& [insertColumns] : this->values) {
            for (Int i = 0;i < insertColumns.Size(); i++) {
                auto& value = insertColumns[i];

                auto expressionStatus = CompileExpression(context, validationScope, value);
                if (!expressionStatus.IsOk()) return expressionStatus;

                auto returnTypeStatus = this->ValidateReturnType(context, validationScope, value, this->columns[i].name);
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

        auto& catalog = CoreEngine::SystemCatalog::Get();

        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk())
            return tableStatus;

        StatementValidationScope validationScope(context.GetAllocator(), this->_slotCount);
        auto columnsDict = catalog.SelectColumnsToDictionary(
            context.GetAllocator(),
            this->table->_tableId
        );

        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::InsertSlot> insertSlots(
            context.GetAllocator(),
            static_cast<Int>(columnsDict.size())
        );
        insertSlots.AlignSize();
        DataStructures::PolymorphicArray<Expressions::Expression*> defaultExpressions(context.GetAllocator());

        const auto identityColumns =
            catalog.SelectIdentityColumnsByTableIdToDictionary(
                context.GetAllocator(),
                this->table->_tableId
            );

        //validate insert columns existence
        HashSet<Int> statementColumns;
        for (Int i = 0;i < this->columns.Size(); i++){
            const auto& column = this->columns[i];
            Headers::ColumnHeader header;

            //check if columns exist on the table
            if (!columnsDict.TryGetValue(column.name.ToLower(), header)) {
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

            insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::ValueSlot(i);
            statementColumns.Add(header.id);
        }

        for (const auto& header: columnsDict | std::views::values) {
            if (header.isSystem
                || identityColumns.Contains(header.id)
                || statementColumns.Contains(header.id)
            ) continue;

            //Insert the null value
            if (header.isNullable) {
                insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::NullSlot();
                continue;
            }

            auto defaultValue = catalog.SelectDefaultValueByColumnId(context.GetAllocator(), header.id);

            if (defaultValue.columnId == INVALID_COLUMN_ID) {
                return Errors::ValidationStatus::Error(
                    Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                        context.GetAllocator(),
                        header.name
                    )
                );
            }

            const auto slotIndex = InsertStatement::InsertDefaultValue(context.GetAllocator(), defaultExpressions, header, defaultValue);
            insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::DefaultSlot(slotIndex);
        }

        this->insertPlan._slotMap = std::move(insertSlots);
        this->insertPlan._sharedDefaults = std::move(defaultExpressions);

        validationScope._tableColumnsArray[this->table->_slotIndex] = std::move(columnsDict);

        return this->HasSelectStatement()
            ? this->ValidateSelectStatement(context, validationScope)
            : this->ResolveAliases(context, validationScope);
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
            this->insertPlan
        );
    }

    Errors::ValidationStatus CreateSchemaStatement::CompileDerived(QueryContext& context){
        if (CoreEngine::SystemCatalog::Get().SchemaExists(context.GetAllocator(), this->databaseId, DataTypes::StringView::ViewOf(this->name))) {
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
            context._session->sessionId,
            this->databaseId,
            this->name
        );
    }

    UpdateColumn::UpdateColumn()
        : value(nullptr), name(DataTypes::String::Null()){}

    Errors::ValidationStatus UpdateStatement::ValidateReturnType(
        const QueryContext& context,
        StatementValidationScope& validationScope,
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
                const auto& columnsDict = validationScope._tableColumnsArray[this->table->_slotIndex];
                if (columnsDict.Get(update->name.name).isNullable)
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
                SQL_TYPES_NAMES[static_cast<Int>(update->name.returnType)],
                SQL_TYPES_NAMES[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::ValidationStatus UpdateStatement::ResolveAliases(
        QueryContext& context,
        StatementValidationScope& validationScope
    ){
        //Add Base Table to the dictionaries
        validationScope._tableAliasesDict.Add(this->table->GetAlias(context), this->table->_tableId);

        validationScope._tableColumnsArray.SetAllocator(context.GetAllocator());
        validationScope._tableColumnsArray.Resize(this->_slotCount);
        validationScope._tableColumnsArray[this->table->_slotIndex] = CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId);
        validationScope._statement = this;

        //start resolving aliases
        for (const auto& update : this->updates) {
            auto columnAliasStatus = CompileColumnExpression(context, update->name, validationScope);
            if (!columnAliasStatus.IsOk())
                return columnAliasStatus;

            auto expressionStatus = CompileExpression(context, validationScope, update->value);
            if (!expressionStatus.IsOk())
                return expressionStatus;

            auto returnTypeResult = this->ValidateReturnType(context, validationScope, update);
            if (!returnTypeResult.IsOk())
                return returnTypeResult;

            update->value->SetIndex(update->name.ordinalPosition);
        }

        if (this->where.expression == nullptr)
            return Errors::ValidationStatus::Ok();

        if (!this->where.IsValid())
            return Errors::ValidationStatus::Error(
                Messages::INVALID_WHERE_CLAUSE,
                context.GetAllocator()
            );

        return CompileExpression(context, validationScope, this->where.expression);
    }


    Errors::ValidationStatus UpdateStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::NO_TABLE_SPECIFIED,
                context.GetAllocator()
            );

        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk()) return tableStatus;

        StatementValidationScope validationScope(context.GetAllocator(), this->_slotCount);
        return this->ResolveAliases(context, validationScope);
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
        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk()) return tableStatus;

        this->columns.TrySetAllocator(context.GetAllocator());
        this->columnIndices.TrySetAllocator(context.GetAllocator());

        static auto& catalog = CoreEngine::SystemCatalog::Get();

        const auto columnsDict = catalog.SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId);

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

        const auto indexes = catalog.SelectIndexes(context.GetAllocator(), this->table->_tableId);

        for (const auto& index: indexes) {
            const auto indexedColumns = catalog.SelectIndexColumnsByIndexIdToDictionary(
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
            context._session->sessionId,
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
        const auto columnTypeView = DataTypes::StringView::ViewOf(columnTypeToLower);
        if (!COLUMN_TYPENAMES_TO_ENUMS.TryGetValue(columnTypeView, columnType)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_COLUMN_TYPE_SPECIFIED(
                    context.GetAllocator(),
                    newColumn->type.name,
                    columnTypeView
                )
            );
        }

        const auto recordSize = COLUMN_SIZES_BY_TYPENAME.Get(&columnTypeView);

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
        const auto columnTypeView = DataTypes::StringView::ViewOf(columnTypeToLower);
        DataType columnType;
        if (!COLUMN_TYPENAMES_TO_ENUMS.TryGetValue(columnTypeView, columnType)) {
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
                        SQL_TYPES_NAMES[header.dataType],
                        DataTypes::StringView::ViewOf(alterColumn->type.name)
                )
            );
        }

        if (header.recordSize > alterColumn->type.size) {

            return Errors::ValidationStatus::Error(
                Messages::CANNOT_ALTER_COLUMN_TO_NEW_SIZE(
                    context.GetAllocator(),
                    alterColumn->name.name,
                    SQL_TYPES_NAMES[header.dataType],
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
        const auto columnNameView = DataTypes::StringView::ViewOf(columnNameToLower);

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
        const auto constraints = CoreEngine::SystemCatalog::Get().SelectConstraints(context.GetAllocator(), this->table->_tableId);

        for (const auto& constraint: constraints) {
            const auto columns = CoreEngine::SystemCatalog::Get().SelectConstraintColumnsByConstraintIdToDictionary(
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

        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk()) return tableStatus;

        const auto columnsDict = CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId);

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
                context._session->sessionId,
                this->table,
                this->type,
                this->column.newColumn
            );
        case Constants::AlterTableType::AlterColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                context._session->sessionId,
                this->table,
                this->type,
                this->column.alterColumn
            );
        case Constants::AlterTableType::RenameColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                context._session->sessionId,
                this->table,
                this->type,
                this->column.renameColumn
            );
        case Constants::AlterTableType::DropColumn:
            return context._compileContext.Allocate<LogicalAlterTable>(
                context._session->sessionId,
                this->table,
                this->type,
                this->column.dropColumn
            );
        default:
            return nullptr;
        }
    }

    Errors::ValidationStatus CompileExpression(QueryContext& context, Expressions::Expression*& expression){
        if (expression == nullptr)
            return Errors::ValidationStatus::Ok();

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
            case Expressions::ExpressionType::Cast:
                return CompileCastExpression(context, expression->AsCast(), expression);
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
        if (expression == nullptr)
            return Errors::ValidationStatus::Ok();

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
            case Expressions::ExpressionType::Cast:
                return CompileCastExpression(context, expression->AsCast(), expression, statementValidationScope);
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
        const auto leftType = Expressions::GetExpressionReturnType(binaryExpr->left);
        const auto rightType = Expressions::GetExpressionReturnType(binaryExpr->right);
        if (leftType == DataType::Null || rightType == DataType::Null){
            expression = context._compileContext.Allocate<Expressions::ConstantExpression>(Value::Null());
            return Errors::ValidationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        if (!binaryExpr->ValidateOperation()) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_OPERATION_ON_DATATYPES(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        const auto promotedType = PromoteType(leftType, rightType);

        if (leftType != promotedType)
            InsertCastExpression(context, binaryExpr->left, promotedType);
        if (rightType != promotedType)
            InsertCastExpression(context, binaryExpr->right, promotedType);

        FoldBinaryExpression(context, expression);
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

        const auto leftType = Expressions::GetExpressionReturnType(binaryExpr->left);
        const auto rightType = Expressions::GetExpressionReturnType(binaryExpr->right);
        if (leftType == DataType::Null || rightType == DataType::Null){
            expression = context._compileContext.Allocate<Expressions::ConstantExpression>(Value::Null());
            return Errors::ValidationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        if (!binaryExpr->ValidateOperation()) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_OPERATION_ON_DATATYPES(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        const auto promotedType = PromoteType(leftType, rightType);

        if (leftType != promotedType)
            InsertCastExpression(context, binaryExpr->left, promotedType);
        if (rightType != promotedType)
            InsertCastExpression(context, binaryExpr->right, promotedType);

        FoldBinaryExpression(context, expression);
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
        if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->right));

        FoldLogicalExpression(context, expression);
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
        if (!logicalExpr->IsNot() && !ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(logicalExpr->right));

        FoldLogicalExpression(context, expression);
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

        FoldFunctionExpression(context, expression);
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

        FoldFunctionExpression(context, expression);
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

        FoldBranchExpression(context, expression);
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

        FoldBranchExpression(context, expression);
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
        if (DataTypes::StringView::ViewOf(column->alias) == WILDCARD) {

            auto* selectStatement = dynamic_cast<SelectStatement*>(statementValidationScope._statement);

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
        DataType outType;
        if (!context._scope.variables.TryGetValue(variableExpr->normalizedName, outType)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_VARIABLE(
                    context.GetAllocator(),
                    variableExpr->name
                )
            );
        }

        variableExpr->dataType = outType;
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnExpression(
        const QueryContext &context,
        ColumnName &column,
        StatementValidationScope& statementValidationScope
    ){
        if (!column.alias.Empty()) {
            UnsignedSmallInt slotIndex;
            if (!statementValidationScope._tableAliasesDict.TryGetValue(column.alias, slotIndex)) {
                return Errors::ValidationStatus::Error(
                    Messages::INVALID_TABLE_ALIAS(
                           context.GetAllocator(),
                           column.alias
                    )
                );
            }

            column._slotIndex = slotIndex;
        }

        bool columnExistsOnTable = false;
        Headers::ColumnHeader columnHeader;
        for (Int slotIndex = 0;slotIndex < statementValidationScope._tableColumnsArray.Size();slotIndex++){
            const auto& columns = statementValidationScope._tableColumnsArray[slotIndex];
            const auto columnNameToLower = column.name.ToLower();
            if (!columns.TryGetValue(columnNameToLower, columnHeader))
                continue;

            if (!columnExistsOnTable) {
                columnExistsOnTable = true;

                column._slotIndex = slotIndex;
                column.columnId = columnHeader.id;
                column.ordinalPosition = columnHeader.ordinalPosition;
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
                    DataTypes::StringView::ViewOf(lastPathSegment->_key)
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
                    DataTypes::StringView::ViewOf(lastPathSegment->_key)
                )
            );

        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileCastExpression(
        QueryContext& context,
        Expressions::CastExpression* castExpr,
        Expressions::Expression*& expression
    ){
        auto result = CompileExpression(context, castExpr->childExpr);
        if (!result.IsOk()) return result;

        const auto childReturnType = Expressions::GetExpressionReturnType(castExpr->childExpr);
        if (castExpr->targetType == childReturnType){
            expression = castExpr->childExpr;
            return Errors::ValidationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(castExpr->targetType, castExpr->childExpr)){
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    childReturnType,
                    castExpr->targetType
                )
            );
        }

        FoldCastExpression(context, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileCastExpression(
        QueryContext& context,
        Expressions::CastExpression* castExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    ){
        auto result = CompileExpression(context, statementValidationScope, castExpr->childExpr);
        if (!result.IsOk()) return result;

        const auto childReturnType = Expressions::GetExpressionReturnType(castExpr->childExpr);
        if (castExpr->targetType == childReturnType){
            expression = castExpr->childExpr;
            return Errors::ValidationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(castExpr->targetType, castExpr->childExpr)){
            return Errors::ValidationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    childReturnType,
                    castExpr->targetType
                )
            );
        }

        FoldCastExpression(context, expression);
        return Errors::ValidationStatus::Ok();
    }

    Errors::ValidationStatus CompileColumnWhenTableAliasExists(
        const QueryContext& context,
        Expressions::ColumnExpression *column,
        const StatementValidationScope& statementValidationScope
    ){
        Headers::ColumnHeader columnHeader;
        if (!statementValidationScope._tableAliasesDict.TryGetValue(column->tableAlias, column->_slotIndex)) {
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                            context.GetAllocator(),
                            column->tableAlias
                )
            );
        }

        const auto& columns = statementValidationScope._tableColumnsArray[column->_slotIndex];
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
        column->ordinalPosition = columnHeader.ordinalPosition;
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

        for (Int slotIndex = 0;slotIndex < statementValidationScope._tableColumnsArray.Size();slotIndex++){
            const auto& columns = statementValidationScope._tableColumnsArray[slotIndex];
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

            column->_slotIndex = slotIndex;
            column->tableId = columnHeader.tableId;
            column->columnId = columnHeader.id;
            column->returnType = static_cast<DataType>(columnHeader.dataType);
            column->ordinalPosition = columnHeader.ordinalPosition;
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
        const StatementValidationScope& validationScope,
        SelectStatement* statement
    ){
        if (DataTypes::StringView::ViewOf(column->alias) != WILDCARD)
            return Errors::ValidationStatus::Ok();

        if (validationScope._indexPos == nullptr)
            return Errors::ValidationStatus::Error(
                Messages::UNEXPECTED_ERROR,
                context.GetAllocator()
            );

        //if no alias is specified get all the columns from the existing tables in the query
        if (column->tableAlias.Empty()) {
            statement->_projections.erase(statement->_projections.begin() + *validationScope._indexPos);

            for (Int slotIndex = 0; slotIndex < validationScope._tableColumnsArray.Size(); slotIndex++){
                const auto& columns = validationScope._tableColumnsArray[slotIndex];
                AssignColumnsFromWildCardExpression(
                    context,
                    columns,
                    column->tableAlias,
                    validationScope,
                    statement->_projections,
                    slotIndex
                );
                *validationScope._indexPos += static_cast<Int>(columns.size());
            }

            return Errors::ValidationStatus::Ok();
        }

        //else get only from the specified
        UnsignedSmallInt slotIndex = 0;
        if (!column->tableAlias.Empty()
            && !validationScope._tableAliasesDict.TryGetValue(column->tableAlias, slotIndex)
        ){
            return Errors::ValidationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                    context.GetAllocator(),
                    column->tableAlias
                )
            );
        }

        //remove the wildcard
        statement->_projections.erase(statement->_projections.begin() + *validationScope._indexPos);
        AssignColumnsFromWildCardExpression(
            context,
            validationScope._tableColumnsArray[slotIndex],
            column->tableAlias,
            validationScope,
            statement->_projections,
            slotIndex
        );

        return Errors::ValidationStatus::Ok();
    }

    void AssignColumnsFromWildCardExpression(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader> &columnsDict,
        const DataTypes::String& tableAlias,
        const StatementValidationScope& statementValidationScope,
        DataStructures::PolymorphicArray<Expressions::Expression*>& results,
        const UnsignedSmallInt slotIndex
    ) {
        results.Insert(nullptr, *statementValidationScope._indexPos, columnsDict.size());
        for (const auto &header: columnsDict | std::views::values) {
            auto* columnExpression = context._compileContext.Allocate<Expressions::ColumnExpression>(
                header.name,
                tableAlias
            );

            columnExpression->_slotIndex = slotIndex;
            columnExpression->name = header.name;
            columnExpression->columnId = header.id;
            columnExpression->tableId = header.tableId;
            columnExpression->ordinalPosition = header.ordinalPosition;
            columnExpression->returnType = static_cast<DataType>(header.dataType);

            const auto insertPos = *statementValidationScope._indexPos + header.ordinalPosition;

            results[insertPos] = columnExpression;
        }
    }

    void FoldExpression(const QueryContext& context, Expressions::Expression *&expression) {
        if (expression == nullptr)
            return;

        switch (expression->expressionType) {
            case Expressions::ExpressionType::Binary:
                FoldBinaryExpression(context, expression);
                break;
            case Expressions::ExpressionType::Logical:
                FoldLogicalExpression(context, expression);
                break;
            case Expressions::ExpressionType::Branch:
                FoldBranchExpression(context, expression);
                break;
            case Expressions::ExpressionType::Function:
                FoldFunctionExpression(context, expression);
                break;
            case Expressions::ExpressionType::Expression:
            case Expressions::ExpressionType::Column:
            case Expressions::ExpressionType::Constant:
            case Expressions::ExpressionType::Variable:
            case Expressions::ExpressionType::Json:
                break;
            case Expressions::ExpressionType::Cast:
                FoldCastExpression(context, expression);
                break;
        }
    }

    void FoldBinaryExpression(
        const QueryContext& context,
        Expressions::Expression *&expression
    ){
        auto* binaryExpr = expression->AsBinary();

        FoldExpression(context, binaryExpr->left);
        FoldExpression(context, binaryExpr->right);

        if (!binaryExpr->left->IsConstant()
            || !binaryExpr->right->IsConstant()
        ) return;

        EvaluateExpression(context, expression);
    }

    void FoldLogicalExpression(
        const QueryContext& context,
        Expressions::Expression *&expression
    ){
        auto* logicalExpr = expression->AsLogical();

        FoldExpression(context, logicalExpr->left);
        FoldExpression(context, logicalExpr->right);

        const auto dominantValue = logicalExpr->IsOr();

        if ((
                logicalExpr->left->IsConstant()
                && TryPropagateChildExpression(expression, logicalExpr->left, logicalExpr->right, dominantValue)
        ) || logicalExpr->IsNot()
        ) return;

        if (logicalExpr->right->IsConstant())
            TryPropagateChildExpression(expression, logicalExpr->right, logicalExpr->left, dominantValue);
    }

    void FoldFunctionExpression(
        const QueryContext& context,
        Expressions::Expression *&expression
    ){
        auto* functionExpr = expression->AsFunction();
        for (auto& argument : functionExpr->arguments) {
            FoldExpression(context, argument);
            if (!argument->IsConstant())
                return;
        }
        EvaluateExpression(context, expression);
    }

    void FoldBranchExpression(
        const QueryContext& context,
        Expressions::Expression *&expression
    ){
        auto* branchExpr = expression->AsBranch();
        for (auto& branch : branchExpr->branches){
            FoldExpression(context, branch);
            if (!branch->IsConstant()) return;

            if (branch->AsConstant()->value.AsBool()) {
                PropagateExpression(expression, branch);
                FoldExpression(context, expression);
                return;
            }
        }
    }

    void FoldCastExpression(
        const QueryContext& context,
        Expressions::Expression*& expression
    ){
        auto* castExpr = expression->AsCast();
        FoldExpression(context, castExpr->childExpr);
        if (castExpr->childExpr->IsConstant())
            EvaluateExpression(context, expression);
    }

    void PropagateExpression(Expressions::Expression *&expression, Expressions::Expression *&childExpr) {
        auto* expr = childExpr;
        childExpr = nullptr;
        expression = expr;
    }

    void EvaluateExpression(const QueryContext& context, Expressions::Expression *&expression) {
        Expressions::BindExpressionRowKernel(expression);
        auto value = Expressions::EvaluateExpression(expression, Expressions::EvaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Constant,
            context.GetAllocator())
        );
        expression = context._compileContext.Allocate<Expressions::ConstantExpression>(value);
    }

    void AssignConstantToExpression(const QueryContext& context, Expressions::Expression *&expression) {
        auto value = Value(true, 0);
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
            case Expressions::ExpressionType::Cast:
                AssignColumnIndicesToCastExpression(columnIndicesDictionary, expression->AsCast());
                break;
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
        expression->ordinalPosition = columnIndicesDictionary.Get(expression->columnId);
    }

    void AssignColumnIndicesToCastExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::CastExpression* castExpr
    ){
        AssignColumnIndicesToExpression(columnIndicesDictionary, castExpr->childExpr);
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
            case Expressions::ExpressionType::Json:
                return CompilePostProjectionJsonExpression(context, expression->AsJson(), postProjectionAliases);
                break;
            case Expressions::ExpressionType::Cast:
                return CompilePostProjectionCastExpression(context, expression->AsCast(), postProjectionAliases);
                break;
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

    Errors::ValidationStatus CompilePostProjectionJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    ){
        return CompilePostProjectionColumnExpression(context, expression->columnPtr, postProjectionAliases);
    }

    Errors::ValidationStatus CompilePostProjectionCastExpression(
        const QueryContext& context,
        const Expressions::CastExpression* castExpr,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    ){
        return CompilePostProjectionExpression(context, castExpr->childExpr, postProjectionAliases);
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
            case Expressions::ExpressionType::Json:
                AssignPostProjectionIndicesToColumnExpression(columnIndicesDictionary, expression->AsJson()->columnPtr);
                break;
            case Expressions::ExpressionType::Cast:
                AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->AsCast()->childExpr);
                break;
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
        expression->ordinalPosition = columnIndicesDictionary.Get(expression->alias);
    }

    Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(const QueryContext& context, const DataType type) {
      return Errors::ValidationStatus::Error(
        Messages::CLAUSE_CANNOT_BE_EVALUATED_TO_BOOLEAN(
            context._compileContext.GetAllocator(),
            SQL_TYPES_NAMES[static_cast<Int>(type)]
        )
      );
    }

    void InsertCastExpression(
        const QueryContext& context,
        Expressions::Expression*& expression,
        DataType type
    ){
        expression = context._compileContext.Allocate<Expressions::CastExpression>(expression, type, false);
    }

    bool TryPropagateChildExpression(
        Expressions::Expression*& expression,
        Expressions::Expression*& leftExpr,
        Expressions::Expression*& rightExpr,
        const bool dominantValue
    ){
        const auto* left = leftExpr->AsConstant();
        if (left->value.IsNull())
            return false;

        if (!DataTypes::Coercions::CanBeParsedToType(DataType::Bool, left->value))
            return false;

        if( left->value.AsBool() == dominantValue)
            PropagateExpression(expression, leftExpr);
        else
            PropagateExpression(expression, rightExpr);

        return true;
    }
}
