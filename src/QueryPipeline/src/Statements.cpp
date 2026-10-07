#include <QueryPipeline/Statements.h>

#include <CoreEngine/Database.h>
#include <Systemic/Coercions/Coercions.h>
#include <Systemic/Functions/StringFunctions.h>
#include <Server/Server.h>
#include <QueryPipeline/LogicalPlan.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <ranges>
#include <QueryPipeline/ValidationMessages.h>

#include <QueryPipeline/Optimizer.h>
#include <QueryPipeline/Parser.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>
#include <CoreEngine/DataStorage/Row/SerializedRow.h>

namespace QueryPipeline::Statements {
    Statement::Statement()
        : table(nullptr), databaseId(Constants::SYSTEM_CATALOG_ID), _slotCount(DEFAULT_SLOT_INDEX){}

    Errors::CompilationStatus Statement::CompileBase(const QueryContext& context)const{
        Errors::CompilationStatus compilationStatus(context.GetAllocator());

        if (!context._session || !context._session->user || !context._session->user->role){
            compilationStatus._code = Errors::CompilationError::Error;
            compilationStatus._message = DataTypes::String::FromView(
                Messages::FAILED_TO_FETCH_USER_SESSION,
                context.GetAllocator()
            );

            return compilationStatus;
        }

        const auto* user = context._session->user;
        const auto requiredPermissions = this->RequiredPermissions();
        if (!user->role->HasPermission(requiredPermissions)) {
            DataTypes::String missing(context.GetAllocator());

            Reflection::ForEachSetFlag(requiredPermissions & ~user->role->permission, [&](const std::string_view name){
                if (!missing.Empty())
                    missing.Append(',');
                missing.Append(name);
            });

            compilationStatus._code = Errors::CompilationError::Error;
            compilationStatus._message = DataTypes::String::Concat(
                context.GetAllocator(),
                "User ", user->name, " is not authorized, missing permission(s): ",
                DataTypes::StringView::ViewOf(missing)
            );
        }

        return compilationStatus;
    }

    Errors::CompilationStatus Statement::Compile(QueryContext& context){
        auto result = this->CompileBase(context);
        if (!result.IsOk())
            return result;

        return this->CompileDerived(context);
    }

   DeclareVariableStatement::DeclareVariableStatement()
       : expression(nullptr), type(DataType::Null){}

    Errors::CompilationStatus DeclareVariableStatement::CompileDerived(QueryContext& context) {
        const auto variableType = this->variable.GetType();

        auto validationStatus = Errors::CompilationStatus(context.GetAllocator());
        if (this->expression) {
            CompilationScope compilationScope;
            auto res = CompileNode(context, this->expression, compilationScope);

            if (!res.IsOk())
                return res;

            if (variableType != DataType::Null && !ValidateExpressionCoercionTypes(variableType, this->expression)) {
                validationStatus._code = Errors::CompilationError::Error;
                validationStatus._message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
            Expressions::GetExpressionReturnType(this->expression),
            variableType
                );

                return validationStatus;
            }

            if (variableType == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        const auto normalizedNameView = DataTypes::StringView::ViewOf(this->variable.GetNormalizedName());
        context._scope.variables.ForceAdd(normalizedNameView, variableType);
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

    Errors::CompilationStatus SetVariableStatement::CompileDerived(QueryContext& context) {
        const auto datatype = this->variable.GetType();

        auto validationStatus = Errors::CompilationStatus(context.GetAllocator());
        if (this->expression) {
            CompilationScope compilationScope;
            auto res = CompileNode(context, this->expression, compilationScope);

            if (!res.IsOk()) return res;

            if (datatype != DataType::Null && !ValidateExpressionCoercionTypes(datatype, this->expression)) {
                validationStatus._code = Errors::CompilationError::Error;
                validationStatus._message = Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    Expressions::GetExpressionReturnType(this->expression),
                    datatype
                );

                return validationStatus;
            }

            if (datatype == DataType::Null)
                this->variable.SetType(Expressions::GetExpressionReturnType(this->expression));
        }

        const auto normalizedNameView = DataTypes::StringView::ViewOf(this->variable.GetNormalizedName());
        context._scope.variables.ForceAdd(normalizedNameView, datatype);
        return validationStatus;
    }

  constexpr Security::Permission SetVariableStatement::RequiredPermissions() const {
    return Constants::DB_WRITER_PERMISSIONS;
  }

  LogicalPlan * SetVariableStatement::ToLogical(QueryContext& context) {
    return context._compileContext.Allocate<LogicalDeclareVariable>(context._session->sessionId, this->variable, this->expression);
  }

  Errors::CompilationStatus CreateUserStatement::CompileDerived(QueryContext& context){
    if (this->username.Empty())
        return Errors::CompilationStatus::Error(Messages::EMPTY_USERNAME, context.GetAllocator());
    if (this->password.Empty())
        return Errors::CompilationStatus::Error(Messages::EMPTY_PASSWORD, context.GetAllocator());
    if (this->role.Empty())
        return Errors::CompilationStatus::Error(Messages::EMPTY_ROLE, context.GetAllocator());
    if (Network::Server::Get().UserExists(this->username))
        return Errors::CompilationStatus::Error(Messages::USER_ALREADY_EXISTS, context.GetAllocator());
    if (!Network::Server::Get().RoleExists(this->role))
        return Errors::CompilationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());

    return Errors::CompilationStatus(context.GetAllocator());
  }

  constexpr Security::Permission CreateUserStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

    LogicalPlan* CreateUserStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalCreateUser>(context._session->sessionId, this->username, this->password, this->role);
    }

    Errors::CompilationStatus GrantRoleStatement::CompileDerived(QueryContext& context){
        if (!Network::Server::Get().UserExists(this->username))
            return Errors::CompilationStatus::Error(Messages::USER_DOES_NOT_EXIST, context.GetAllocator());
        if (!Network::Server::Get().RoleExists(this->role))
            return Errors::CompilationStatus::Error(Messages::FAILED_TO_FETCH_ROLE, context.GetAllocator());
        return Errors::CompilationStatus(context.GetAllocator());
    }

    constexpr Security::Permission GrantRoleStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan * GrantRoleStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalGrantRole>(context._session->sessionId, this->username, this->role);
    }

    Errors::CompilationStatus DeleteStatement::CompileDerived(QueryContext& context){
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

    Errors::CompilationStatus JoinStatement::CompileDerived(QueryContext& context){
        return Errors::CompilationStatus(context.GetAllocator());
    }

    Errors::CompilationStatus JoinStatement::Compile(const QueryContext& context, const Int databaseId, UnsignedSmallInt& slotCount){
        this->databaseId = databaseId;
        if (this->table == nullptr)
            return Errors::CompilationStatus::Error(
                Messages::MISSING_TABLE_IN_JOIN,
                context.GetAllocator()
            );

        return this->table->Compile(context, this->databaseId, slotCount);
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

    CompilationScope::CompilationScope(
    const Memory::IAllocator* allocator,
    const UnsignedSmallInt numberOfTables
    ):  _tableColumnsArray(allocator, numberOfTables), _indexPos(nullptr), _statement(nullptr){
        _tableColumnsArray.AlignSize();
    }

    CompilationScope::CompilationScope(
        Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>>& tableColumnsArray,
        Statement *statement,
        int *indexPos
    ):  _tableAliasesDict(std::move(tableAliasesDictionary)),
        _tableColumnsArray(std::move(tableColumnsArray)),
        _indexPos(indexPos), _statement(statement) {}

    CompilationScope::CompilationScope(
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

    Errors::CompilationStatus Identity::Validate(const QueryContext& context) const{
        if (this->incrementFactor <= 0)
            return Errors::CompilationStatus::Error(
                Messages::INVALID_INCREMENT_FACTOR,
                context.GetAllocator()
            );
        return Errors::CompilationStatus(context.GetAllocator());
    }

    bool NewColumn::HasIdentity()const{ return this->identity != nullptr;}

    OrderColumn::OrderColumn()
        : expression(nullptr), outputIndex(INVALID_ORDINAL_POS), type(Constants::OrderType::ASCENDING) {}

    AlterColumn::AlterColumn(){
        this->columnId = INVALID_COLUMN_ID;
        this->index = INVALID_ORDINAL_POS;
    }

    WhereClause::WhereClause() { this->expression = nullptr; }

    bool WhereClause::IsValid() const{
        return (this->expression == nullptr)
            || this->expression->Is<Expressions::LogicalExpression>()
            || this->expression->Is<Expressions::BinaryExpression>();
    }

    bool OrderByStatement::Validate(
        const DataStructures::PolymorphicArray<OrderColumn*>& selectColumns,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& columnsDict
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
        //   CoreEngine::Catalog::ColumnHeader header;
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

    Errors::CompilationStatus DataSource::Compile(
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
            return Errors::CompilationStatus::Error(
                Messages::INVALID_TABLE(context.GetAllocator(), this->GetFullName(context))
            );
        }

        this->_tableId = tableHeader.id;
        this->_ordinalPosition = tableHeader.ordinalPosition;
        this->_databaseId = tableHeader.databaseId;
        this->_slotIndex = outSlotCount++;

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus DataSource::ValidateTableCreate(const QueryContext& context, const Int selectedDatabaseId){
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
            return Errors::CompilationStatus::Error(
            Messages::TABLE_ALREADY_EXISTS(context.GetAllocator(), this->GetFullName(context))
            );

        this->_databaseId = selectedDatabaseId;
        return Errors::CompilationStatus::Ok();
    }

    CreateTableStatement::CreateTableStatement(){
        this->table = nullptr;
        this->constraint = nullptr;
    }

    Errors::CompilationStatus CreateTableStatement::CompileSchema(const QueryContext& context) const{
        const auto& schemasDict = CoreEngine::SystemCatalog::Get().SelectSchemasToDictionary(context.GetAllocator(), this->databaseId);
        CoreEngine::Catalog::SchemaHeader schemaHeader;

        this->table->schema.ToLowerInPlace();
        if (!schemasDict.TryGetValue(this->table->schema, schemaHeader))
            return Errors::CompilationStatus::Error(
                Messages::SCHEMA_DOES_NOT_EXIST(context.GetAllocator(), this->table->schema)
            );

        this->table->_schemaId = schemaHeader.id;
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus CreateTableStatement::CompileColumnExpression(
        const QueryContext& context,
        NewColumn*& column,
        Dictionary<DataTypes::String, column_index_t>& columnNamesToIndexes,
        bool& primaryKeyFound,
        column_index_t& index
    ){
        UnsignedSmallInt columnSize;
        std::ostringstream os;

        column->type.name.ToLowerInPlace();
        if (!ColumnSizesByName::TryGetValue(DataTypes::StringView::ViewOf(column->type.name), columnSize)) {
            return Errors::CompilationStatus::Error(
                Messages::DATATYPE_DOES_NOT_EXIST(context.GetAllocator(), column->type.name)
            );
        }

        if (columnSize != 0)
            column->type.size = columnSize;

        const auto dataType = ColumnTypesByName::Get(DataTypes::StringView::ViewOf(column->type.name));

        if (dataType == DataType::Decimal) {
            if (!column->type.decimal.Validate()) {
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_DECIMAL_DECLARATION,
                    context.GetAllocator()
                );
            }

            column->type.size = DataTypes::Decimal::Size(column->type.decimal.precision);
        }

        column->index = index++;

        columnNamesToIndexes.Add(column->name.name, column->index);

        if(column->isPrimaryKey && primaryKeyFound){
            return Errors::CompilationStatus::Error(
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

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus CreateTableStatement::CompileDerived(QueryContext& context){
        auto result = this->table->ValidateTableCreate(context, this->databaseId);
        if (!result.IsOk()) return result;

        result = this->CompileSchema(context);
        if (!result.IsOk()) return result;

        this->columns.TrySetAllocator(context.GetAllocator());
        this->primaryKey.TrySetAllocator(context.GetAllocator());

        column_index_t indexPosition = 0;
        bool primaryKeyFound = false;
        Dictionary<DataTypes::String, column_index_t> columnNamesToIndexes;

        if (this->columns.Size() > Constants::MAX_TABLE_COLUMNS)
            return Errors::CompilationStatus::Error(
            Messages::MAX_NUMBER_OF_COLUMNS_EXCEEDED,
                    context.GetAllocator()
            );

        for (auto* column: this->columns){
            auto columnResult = this->CompileColumnExpression(
                context,
                column,
                columnNamesToIndexes,
                primaryKeyFound,
                indexPosition
            );

            if (!columnResult.IsOk())
                return columnResult;
        }

        if (this->constraint == nullptr)
            return Errors::CompilationStatus::Ok();

        if (primaryKeyFound) {
            return Errors::CompilationStatus::Error(
                Messages::PRIMARY_KEY_AND_CONSTRAINT_DECLARED,
                context.GetAllocator()
            );
        }

        //primary key will be clear for sure here
        for (const auto& column: this->constraint->columns)
            this->primaryKey.Push(columnNamesToIndexes[column.name]);

        return Errors::CompilationStatus::Ok();
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
        :   Statement(), columnHeaders(allocator),
            _projections(allocator), _joins(allocator),
            orderBy(nullptr), top(INVALID_TOP),
            _visibleProjectionCount(0), distinct(false){}

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

    Errors::CompilationStatus SelectStatement::CompileNoTableStatement(QueryContext& context){
        CompilationScope compilationScope;
        for (auto& resultExpr : this->_projections){
            auto exprResult = CompileNode(context, resultExpr, compilationScope);

            if (!exprResult.IsOk()) return exprResult;
        }

        if (this->HasJoins())
            return Errors::CompilationStatus::Error(
                Messages::JOIN_WITH_NO_BASE_TABLE_SELECT,
                context.GetAllocator()
            );

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus SelectStatement::Compile(
        QueryContext& context,
        Dictionary<DataTypes::String, table_id_t>& aliasesDict
    ){
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>> tableColumnsArray(
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

        CompilationScope compilationScope(
            aliasesDict,
            tableColumnsArray,
            this
        );

        //start resolving aliases
        for (auto i = 0; i < this->_projections.Size(); i++) {
            auto* projection = this->_projections[i];

            // wildcards resize _projections, so they are expanded here instead of inside CompileNode,
            // which keeps a reference to the slot across its passes
            if (projection->Is<Expressions::ColumnExpression>()
                && DataTypes::StringView::ViewOf(projection->As<Expressions::ColumnExpression>()->alias) == WILDCARD
            ){
                const auto sizeBefore = this->_projections.Size();
                auto position = i;
                compilationScope._indexPos = &position;

                auto wildcardStatus = CompileWildcard(context, projection->As<Expressions::ColumnExpression>(), compilationScope, this);
                if (!wildcardStatus.IsOk())
                    return wildcardStatus;

                // the wildcard was replaced by (newSize - oldSize + 1) bound columns, continue after them
                i += static_cast<Int>(this->_projections.Size() - sizeBefore);
                continue;
            }

            auto compileStatus = CompileNode(context, this->_projections[i], compilationScope);
            if (!compileStatus.IsOk())
                return compileStatus;
        }

        this->_visibleProjectionCount = this->_projections.Size();

        auto result = this->CompileWhereClause(context, compilationScope);
        if (!result.IsOk()) return result;

        //validate join expressions
        compilationScope._indexPos = nullptr;
        for (const auto& join: this->_joins) {
            auto compileStatus = CompileNode(context, join->expression, compilationScope);
            if (!compileStatus.IsOk())
                return compileStatus;
        }

        if (this->orderBy == nullptr)
            return Errors::CompilationStatus::Ok();

        Dictionary<DataTypes::String, Int> aliasesDictionary;
        for (auto i = 0;i < this->_visibleProjectionCount; i++){
            const auto& projectionExprName = this->_projections[i]->name;

            if (projectionExprName.Empty())
                continue;

            if (aliasesDictionary.Contains(projectionExprName)) {
                return Errors::CompilationStatus::Error(
                    Messages::DUPLICATE_RESULT_COLUMN_NAMES,
                    context.GetAllocator()
                );
            }

            aliasesDictionary.Add(projectionExprName, i);
        }

        for (auto* column: this->orderBy->columns) {
            auto compileStatus = ResolveOrderByExpression(
                context,
                column,
                aliasesDictionary,
                this->_visibleProjectionCount
            );
            if (!compileStatus.IsOk())
                return compileStatus;
        }

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus SelectStatement::CompileWhereClause(
        QueryContext &context,
        CompilationScope& compilationScope
    ) {
        if (!this->HasWhere()) return Errors::CompilationStatus::Ok();

        if (!this->where.IsValid())
            return Errors::CompilationStatus::Error(
                Messages::INVALID_WHERE_CLAUSE,
                context.GetAllocator()
            );

        auto expressionResult = CompileNode(context, this->where.expression, compilationScope);
        if (!expressionResult.IsOk()) return expressionResult;

        if (!ValidateExpressionCoercionTypes(DataType::Bool, this->where.expression))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(this->where.expression));

        return Errors::CompilationStatus::Ok();
    }

    LogicalPlan* SelectStatement::BuildTableScanPlan(
        const QueryContext& context,
        DataSource* table,
        const PredicatePushDownResult& predicatesResult
    ){
        auto* current = context._compileContext.Allocate<LogicalTableScan>(table, predicatesResult.PushDownFilter(table->_tableId));
        return context._compileContext.Allocate<LogicalMaterialize>(
            current,
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

     LogicalPlan* SelectStatement::BuildOrderByStatement(
         const QueryContext& context,
         LogicalPlan* current,
         const Dictionary<DataTypes::String, column_index_t>& postProjectionIndicesDictionary
     ) const{
         if(this->orderBy == nullptr)
             return nullptr;

         for (const auto* column : this->orderBy->columns)
             AssignPostProjectionIndicesToExpression(postProjectionIndicesDictionary, column->expression);

         return context._compileContext.Allocate<LogicalOrder>(current, this->orderBy->columns);
     }

    Errors::CompilationStatus SelectStatement::CompileDerived(QueryContext& context){
        if (!this->_joins.Empty() && this->table == nullptr) {
            return Errors::CompilationStatus::Error(
                Messages::JOIN_WITH_NO_BASE_TABLE_SELECT,
                context.GetAllocator()
            );
        }

        //resolve expressions here since no column is to be used
        if (this->table == nullptr)
            return this->CompileNoTableStatement(context);

        auto tableResult = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableResult.IsOk())
            return tableResult;

        for (auto* join: this->_joins) {
            auto joinResult = join->Compile(context, this->databaseId, this->_slotCount);
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

        if (this->distinct)
            current = context._compileContext.Allocate<LogicalDistinct>(current);

        if (this->orderBy)
            current = this->BuildOrderByStatement(context, current, postProjectionIndicesDictionary);

        if (this->HasTopStatement())
            current = context._compileContext.Allocate<LogicalTop>(current, this->top);

        return current;
    }

    Errors::CompilationStatus CreateDbStatement::CompileDerived(QueryContext& context){
        if (CoreEngine::SystemCatalog::Get().DatabaseExists(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name))) {
            return Errors::CompilationStatus::Error(
                Messages::DATABASE_ALREADY_EXISTS(context.GetAllocator(), this->name)
            );
        }

        return Errors::CompilationStatus::Ok();
    }

    constexpr Security::Permission CreateDbStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan* CreateDbStatement::ToLogical(QueryContext& context){
        return context._compileContext.Allocate<LogicalCreateDatabase>(context._session->sessionId, this->name);
    }

    Errors::CompilationStatus DropDbStatement::CompileDerived(QueryContext& context){
        const auto database = CoreEngine::SystemCatalog::Get().SelectDatabase(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name));

        if (database.name.Empty()) {
            return Errors::CompilationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_DROP(
                    context.GetAllocator(),
                    DataTypes::StringView::ViewOf(this->name)
                )
            );
        }

        if (database.isSystem) {
            return Errors::CompilationStatus::Error(
                Messages::CANNOT_DROP_SYSTEM_DATABASE(
                    context.GetAllocator(),
                    DataTypes::StringView::ViewOf(this->name)
                )
            );
        }

        return Errors::CompilationStatus::Ok();
    }

    constexpr Security::Permission DropDbStatement::RequiredPermissions() const{
        return Constants::ADMIN_PERMISSIONS;
    }

    LogicalPlan * DropDbStatement::ToLogical(QueryContext& context){
        return nullptr;
    }

    Errors::CompilationStatus UseDatabaseStatement::CompileDerived(QueryContext& context){
        const auto dbHeader = CoreEngine::SystemCatalog::Get().SelectDatabase(
            context.GetAllocator(),
            DataTypes::StringView::ViewOf(this->name)
        );

        if (dbHeader.id == INVALID_DATABASE_ID) {
            return Errors::CompilationStatus::Error(
                Messages::DATABASE_DOES_NOT_EXIST_ON_USE(context.GetAllocator(), DataTypes::StringView::ViewOf(this->name))
            );
        }

        this->databaseId = dbHeader.id;
        return Errors::CompilationStatus::Ok();
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
        const CoreEngine::Catalog::ColumnHeader &header,
        CoreEngine::Catalog::DefaultValuesHeader& defaultValue
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

    void InsertStatement::InsertNullValues(const QueryContext& context, const CoreEngine::Catalog::ColumnHeader& header){
        for (auto& [insertColumns] : this->values){
            auto* expression = context._compileContext.Allocate<Expressions::ConstantExpression>(Value::Null(header.ordinalPosition));
            insertColumns.Push(expression);
        }
    }

    Errors::CompilationStatus InsertStatement::ValidateReturnType(
        const QueryContext& context,
        CompilationScope& compilationScope,
        Expressions::Expression*& expression,
        const DataTypes::String& columnName
    ) const{
        const auto valueType = Expressions::GetExpressionReturnType(expression);

        const auto& columnsDict = compilationScope._tableColumnsArray[this->table->_slotIndex];

        const auto columnNameToLower = columnName.ToLower();
        const auto& columnHeader = columnsDict.Get(columnNameToLower);

        const auto columnType = static_cast<DataType>(columnHeader.dataType);

        if (valueType == columnType)
            return Errors::CompilationStatus::Ok();

        if (DataTypes::Coercions::IsCoercionAllowed(
            valueType,
            columnType
        )){
            InsertCastExpression(context, expression, columnType);
            return FoldNode(context, expression->As<Expressions::CastExpression>(), expression);
        }

        if (expression->Is<Expressions::ConstantExpression>()) {
            auto* constantExpr = expression->As<Expressions::ConstantExpression>();

            if (constantExpr->value.IsNull()) {
                if (columnHeader.isNullable){
                    constantExpr->value.SetType(columnType);
                    return Errors::CompilationStatus::Ok();
                }

                return Errors::CompilationStatus::Error(
                    Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                        context.GetAllocator(),
                        columnHeader.name
                    )
                );
            }

            if (DataTypes::Coercions::CanBeParsedToType(columnType, constantExpr->value)){
                InsertCastExpression(context, expression, columnType);
                EvaluateExpression(context, expression);
                return Errors::CompilationStatus::Ok();
            }
        }

        return Errors::CompilationStatus::Error(
            Messages::CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
                context.GetAllocator(),
                columnName,
                SQL_TYPES_NAMES[static_cast<Int>(columnType)],
                SQL_TYPES_NAMES[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::CompilationStatus InsertStatement::ValidateSelectStatement(QueryContext& context, CompilationScope& compilationScope)const{
        if (this->selectStatement == nullptr)
            return Errors::CompilationStatus::Ok();

        this->selectStatement->databaseId = this->databaseId;

        auto selectStatus = this->selectStatement->CompileDerived(context);
        if (!selectStatus.IsOk())
            return selectStatus;

        if (this->selectStatement->_projections.Size() != this->columns.Size())
            return Errors::CompilationStatus::Error(
                Messages::INSERT_STATEMENT_INVALID_NUMBER_OF_ARGUMENTS_ON_SUB_SELECT,
                context.GetAllocator()
            );

        for (Int i = 0;i < this->selectStatement->_projections.Size();i++) {
            auto returnTypeStatus = this->ValidateReturnType(
                context,
                compilationScope,
                this->selectStatement->_projections[i],
                this->columns[i].name
            );

            if (!returnTypeStatus.IsOk())
                return returnTypeStatus;
        }

        return Errors::CompilationStatus::Ok();
    }

    bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

    Errors::CompilationStatus InsertStatement::ResolveAliases(QueryContext& context, CompilationScope& compilationScope){
        compilationScope._tableAliasesDict.Add(this->table->GetAlias(context), this->table->_tableId);

        for (auto& [insertColumns] : this->values) {
            for (Int i = 0;i < insertColumns.Size(); i++) {
                auto& value = insertColumns[i];

                auto expressionStatus = CompileNode(context, value, compilationScope);
                if (!expressionStatus.IsOk()) return expressionStatus;

                auto returnTypeStatus = this->ValidateReturnType(context, compilationScope, value, this->columns[i].name);
                if (!returnTypeStatus.IsOk()) return returnTypeStatus;
            }
        }

        return Errors::CompilationStatus::Ok();
    }

  //TODO validate length of columns to match max record_size from master DB
  Errors::CompilationStatus InsertStatement::CompileDerived(QueryContext& context){
      if (this->table == nullptr)
          return Errors::CompilationStatus::Error(
              Messages::NO_TABLE_SPECIFIED,
              context.GetAllocator()
          );

      static auto& catalog = CoreEngine::SystemCatalog::Get();

      auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
      if (!tableStatus.IsOk())
          return tableStatus;

      CompilationScope compilationScope(context.GetAllocator(), this->_slotCount);
      auto columnsDict = catalog.SelectColumnsToDictionary(
          context.GetAllocator(),
          this->table->_tableId
      );

      const auto* allocator = context.GetAllocator();
      const auto columnCount = static_cast<Int>(columnsDict.size());
      DataStructures::PolymorphicArray<CoreEngine::StorageTypes::InsertSlot> insertSlots(
          allocator,
          columnCount
      );
      DataStructures::PolymorphicArray<DataType> columnTypes(
          allocator,
          columnCount
      );
      DataStructures::PolymorphicArray<CoreEngine::StorageTypes::InsertColumnPlan> columnPlans(
          allocator,
          columnCount
      );

      insertSlots.AlignSize();
      columnTypes.AlignSize();
      columnPlans.AlignSize();

      DataStructures::PolymorphicArray<Expressions::Expression*> defaultExpressions(allocator);
      const auto identityColumns =
          catalog.SelectIdentityColumnsByTableIdToDictionary(
              allocator,
              this->table->_tableId
          );

      //validate insert columns existence
      HashSet<Int> statementColumns;
      for (Int i = 0;i < this->columns.Size(); i++){
          const auto& column = this->columns[i];
          CoreEngine::Catalog::ColumnHeader header;

          //check if columns exist on the table
          if (!columnsDict.TryGetValue(column.name.ToLower(), header)) {
              return Errors::CompilationStatus::Error(
                  Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                      allocator,
                      this->table->GetAlias(context),
                      column.name
                  )
              );
          }

          //check if the specified column is an identity column
          if (identityColumns.Contains(header.id)) {
              return Errors::CompilationStatus::Error(
                  Messages::IDENTITY_COLUMN_ON_INSERT,
                  allocator
              );
          }

          columnTypes[header.ordinalPosition] = static_cast<DataType>(header.dataType);
          insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::ValueSlot(i);

          auto& columnPlan = columnPlans[header.ordinalPosition];
          columnPlan._source = CoreEngine::StorageTypes::InsertColumnSource::Vector;
          columnPlan._slot = i;

          statementColumns.Add(header.id);
      }

      for (const auto& header: columnsDict | std::views::values) {
          auto& columnPlan = columnPlans[header.ordinalPosition];
          columnPlan._type = static_cast<DataType>(header.dataType);
          columnPlan._nullable = header.isNullable;

          if (header.isSystem
              || statementColumns.Contains(header.id)
          ) continue;

          if (identityColumns.Contains(header.id)){
              columnPlan._source = CoreEngine::StorageTypes::InsertColumnSource::Identity;
              continue;
          }

          //Insert the null value
          if (header.isNullable) {
              insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::NullSlot();
              columnPlan._source = CoreEngine::StorageTypes::InsertColumnSource::Null;
              continue;
          }

          auto defaultValue = catalog.SelectDefaultValueByColumnId(allocator, header.id);
          if (defaultValue.columnId == INVALID_COLUMN_ID) {
              return Errors::CompilationStatus::Error(
                  Messages::COLUMN_DOES_NOT_ALLOW_NULLS(
                      allocator,
                      header.name
                  )
              );
          }

          const auto slotIndex = InsertStatement::InsertDefaultValue(allocator, defaultExpressions, header, defaultValue);
          insertSlots[header.ordinalPosition] = CoreEngine::StorageTypes::InsertSlot::DefaultSlot(slotIndex);

          columnPlan._source = CoreEngine::StorageTypes::InsertColumnSource::Default;
          columnPlan._slot = slotIndex;
      }

      this->insertPlan._slotMap = std::move(insertSlots);
      this->insertPlan._sharedDefaults = std::move(defaultExpressions);
      this->insertPlan._columnsPlans = std::move(columnPlans);
      this->valueTypes = std::move(columnTypes);

      compilationScope._tableColumnsArray[this->table->_slotIndex] = std::move(columnsDict);

      return this->HasSelectStatement()
                 ? this->ValidateSelectStatement(context, compilationScope)
                 : this->ResolveAliases(context, compilationScope);
  }

    constexpr Security::Permission InsertStatement::RequiredPermissions() const{
        return Constants::DB_WRITER_PERMISSIONS;
    }

    LogicalPlan* InsertStatement::ToLogical(QueryContext& context) {
        auto* child =  (this->HasSelectStatement())
                           ? this->selectStatement->ToLogical(context)
                           : context._compileContext.Allocate<LogicalValues>(
                               this->values,
                               this->valueTypes
                           );

        return context._compileContext.Allocate<LogicalInsert>(
            this->table,
            child,
            this->insertPlan
        );
    }

    Errors::CompilationStatus CreateSchemaStatement::CompileDerived(QueryContext& context){
        if (CoreEngine::SystemCatalog::Get().SchemaExists(context.GetAllocator(), this->databaseId, DataTypes::StringView::ViewOf(this->name))) {
            return Errors::CompilationStatus::Error(
                Messages::SCHEMA_ALREADY_EXISTS(context.GetAllocator(), this->name)
            );
        }

        return Errors::CompilationStatus::Ok();
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

    Errors::CompilationStatus UpdateStatement::ValidateReturnType(
        const QueryContext& context,
        CompilationScope& compilationScope,
        const UpdateColumn* update
    ) const{
        const auto valueType = Expressions::GetExpressionReturnType(update->value);
        if (
            DataTypes::Coercions::IsCoercionAllowed(
                valueType,
                update->name.returnType
            )
        ) return Errors::CompilationStatus::Ok();

        if (update->value->Is<Expressions::ConstantExpression>()) {
            const auto* constantExpr = update->value->As<Expressions::ConstantExpression>();

            if (constantExpr->value.IsNull()) {
                const auto& columnsDict = compilationScope._tableColumnsArray[this->table->_slotIndex];
                if (columnsDict.Get(update->name.name).isNullable)
                    return Errors::CompilationStatus::Ok();

                return Errors::CompilationStatus::Error(
                    Messages::COLUMN_DOES_NOT_ALLOW_NULLS(context.GetAllocator(), update->name.name)
                );
            }

            if (DataTypes::Coercions::CanBeParsedToType(update->name.returnType, constantExpr->value))
                return Errors::CompilationStatus::Ok();
        }

        return Errors::CompilationStatus::Error(
            Messages::CANNOT_UPDATE_COLUMN_WITH_DATATYPE(
                context.GetAllocator(),
                update->name.name,
                SQL_TYPES_NAMES[static_cast<Int>(update->name.returnType)],
                SQL_TYPES_NAMES[static_cast<Int>(valueType)]
            )
        );
    }

    Errors::CompilationStatus UpdateStatement::ResolveAliases(
        QueryContext& context,
        CompilationScope& compilationScope
    ){
        //Add Base Table to the dictionaries
        compilationScope._tableAliasesDict.Add(this->table->GetAlias(context), this->table->_tableId);

        compilationScope._tableColumnsArray.SetAllocator(context.GetAllocator());
        compilationScope._tableColumnsArray.Resize(this->_slotCount);
        compilationScope._tableColumnsArray[this->table->_slotIndex] = CoreEngine::SystemCatalog::Get().SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId);
        compilationScope._statement = this;

        //start resolving aliases
        for (const auto& update : this->updates) {
            auto columnAliasStatus = BindNode(context, update->name, compilationScope);
            if (!columnAliasStatus.IsOk())
                return columnAliasStatus;

            auto expressionStatus = CompileNode(context, update->value, compilationScope);
            if (!expressionStatus.IsOk())
                return expressionStatus;

            auto returnTypeResult = this->ValidateReturnType(context, compilationScope, update);
            if (!returnTypeResult.IsOk())
                return returnTypeResult;

            update->value->SetIndex(update->name.ordinalPosition);
        }

        if (this->where.expression == nullptr)
            return Errors::CompilationStatus::Ok();

        if (!this->where.IsValid())
            return Errors::CompilationStatus::Error(
                Messages::INVALID_WHERE_CLAUSE,
                context.GetAllocator()
            );

        return CompileNode(context, this->where.expression, compilationScope);
    }


    Errors::CompilationStatus UpdateStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::CompilationStatus::Error(
                Messages::NO_TABLE_SPECIFIED,
                context.GetAllocator()
            );

        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk()) return tableStatus;

        CompilationScope compilationScope(context.GetAllocator(), this->_slotCount);
        return this->ResolveAliases(context, compilationScope);
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

    Errors::CompilationStatus CreateIndexStatement::CompileDerived(QueryContext& context){
        auto tableStatus = this->table->Compile(context, this->databaseId, this->_slotCount);
        if (!tableStatus.IsOk()) return tableStatus;

        this->columns.TrySetAllocator(context.GetAllocator());
        this->columnIndices.TrySetAllocator(context.GetAllocator());

        static auto& catalog = CoreEngine::SystemCatalog::Get();

        const auto columnsDict = catalog.SelectColumnsToDictionary(context.GetAllocator(), this->table->_tableId);

        for(auto& column: this->columns) {
            CoreEngine::Catalog::ColumnHeader header;
            if (columnsDict.TryGetValue(column, header)) {
                this->columnIndices.Push(header.ordinalPosition);
                continue;
            }

            return Errors::CompilationStatus::Error(
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
                return Errors::CompilationStatus::Error(
                    Messages::INDEX_EXISTS(
                        context.GetAllocator(),
                        this->name
                    )
                );
            }
            //check if identical index exists (no need for a duplicate).
        }
        return Errors::CompilationStatus::Ok();
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

    Errors::CompilationStatus AlterTableStatement::CompileAddColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& headers
    )const{
        auto* newColumn = this->column.newColumn;
        const auto columnNameToLower = newColumn->name.name.ToLower();
        if (headers.Contains(columnNameToLower)) {
            return Errors::CompilationStatus::Error(
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
            return Errors::CompilationStatus::Error(
                Messages::DEFAULT_VALUE_NULL_ON_NOT_NULL_COLUMN,
                context.GetAllocator()
            );
        }

        newColumn->index = headers.size();

        DataType columnType;
        const auto columnTypeToLower = newColumn->type.name.ToLower();
        const auto columnTypeView = DataTypes::StringView::ViewOf(columnTypeToLower);
        if (!ColumnTypesByName::TryGetValue(columnTypeView, columnType)) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_COLUMN_TYPE_SPECIFIED(
                    context.GetAllocator(),
                    newColumn->type.name,
                    columnTypeView
                )
            );
        }

        const auto recordSize = ColumnSizesByName::Get(&columnTypeView);

        if (recordSize != 0)
            newColumn->type.size = recordSize;

        if (columnType == DataType::Decimal) {
            if (!newColumn->type.decimal.Validate()) {
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_DECIMAL_DECLARATION,
                    context.GetAllocator()
                );
            }

            newColumn->type.size = DataTypes::Decimal::Size(newColumn->type.decimal.precision);
        }

        //TODO Check this
        // this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus AlterTableStatement::CompileAlterColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& headers
    )const{
        CoreEngine::Catalog::ColumnHeader header;
        std::ostringstream os;

        auto* alterColumn = this->column.alterColumn;

        const auto columnNameToLower = alterColumn->name.name.ToLower();
        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::CompilationStatus::Error(
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
        if (!ColumnTypesByName::TryGetValue(columnTypeView, columnType)) {
            return Errors::CompilationStatus::Error(
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
            return Errors::CompilationStatus::Error(
                Messages::CANNOT_ALTER_COLUMN_TO_TYPE(
                    context.GetAllocator(),
                    alterColumn->name.name,
                    SQL_TYPES_NAMES[header.dataType],
                    DataTypes::StringView::ViewOf(alterColumn->type.name)
                )
            );
        }

        if (header.recordSize > alterColumn->type.size) {

            return Errors::CompilationStatus::Error(
                Messages::CANNOT_ALTER_COLUMN_TO_NEW_SIZE(
                    context.GetAllocator(),
                    alterColumn->name.name,
                    SQL_TYPES_NAMES[header.dataType],
                    alterColumn->type.size
                )
            );
        }

        alterColumn->columnId = header.id;
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus AlterTableStatement::CompileDropColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& headers
    )const{
        CoreEngine::Catalog::ColumnHeader header;

        auto* dropColumn = this->column.dropColumn;
        const auto columnNameToLower = dropColumn->name.name.ToLower();
        const auto columnNameView = DataTypes::StringView::ViewOf(columnNameToLower);

        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::CompilationStatus::Error(
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
                return Errors::CompilationStatus::Error(
                    Messages::CANNOT_DROP_COLUMN_HAS_CONSTRAINTS(
                        context.GetAllocator(),
                        dropColumn->name.name,
                        constraint.name
                    )
                );
            }
        }

        dropColumn->index = header.ordinalPosition;
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus AlterTableStatement::CompileRenameColumn(
        const QueryContext& context,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& headers
    )const{
        CoreEngine::Catalog::ColumnHeader header;

        auto* renameColumn = this->column.renameColumn;
        const auto columnNameToLower = renameColumn->oldName.name.ToLower();

        if (!headers.TryGetValue(columnNameToLower, header)) {
            return Errors::CompilationStatus::Error(
                Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                    context.GetAllocator(),
                    this->table->GetAlias(context),
                    renameColumn->oldName.name
                )
            );
        }

        renameColumn->columnId = header.id;
        renameColumn->ordinalPosition = header.ordinalPosition;

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus AlterTableStatement::CompileDerived(QueryContext& context){
        if (this->table == nullptr)
            return Errors::CompilationStatus::Error(
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
            return Errors::CompilationStatus::Error(
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

    Errors::CompilationStatus CompileNode(
        QueryContext& context,
        Expressions::Expression*& slot,
        CompilationScope& compilationScope
    ){
        auto result = WalkExpressionPostOrder(context, slot, [&]<typename TNode>(TNode* node, Expressions::Expression*&)
        {
           return BindNode(context, node, compilationScope);
        });

        if (!result.IsOk())
            return result;

        result = WalkExpressionPostOrder(context, slot, [&]<typename TNode>(TNode* node, Expressions::Expression*& nodeSlot)
        {
           return TypeCheckNode(context, node,nodeSlot);
        });

        if (!result.IsOk())
            return result;

        return WalkExpressionPostOrder(context, slot, [&]<typename TNode>(TNode* node, Expressions::Expression*& nodeSlot)
        {
           return FoldNode(context, node, nodeSlot);
        });
    }

    Errors::CompilationStatus BindNode(
        const QueryContext&,
        const Expressions::Expression*,
        const CompilationScope&
    ){
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus BindNode(
        QueryContext &context,
        Expressions::BranchExpression *node,
        CompilationScope&
    ) {
        // children are already bound by the walker; type checking and folding index results[i] by branch
        if (!node->ValidateNumberOfArguments())
            return Errors::CompilationStatus::Error(
                Messages::INVALID_NUMBER_OF_ARGUMENTS_ON_BRANCH_EXPRESSION,
                context.GetAllocator()
            );

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus BindNode(
        QueryContext& context,
        Expressions::ColumnExpression* node,
        const CompilationScope& statementValidationScope
    ){
        // top level SELECT wildcards are expanded before compilation, any wildcard reaching here is misplaced
        if (DataTypes::StringView::ViewOf(node->alias) == WILDCARD)
            return Errors::CompilationStatus::Error(
                Messages::WILDCARD_USED_ON_NON_SELECT,
                context.GetAllocator()
            );

        return node->HasTableAlias()
                   ? CompileColumnWhenTableAliasExists(context, node, statementValidationScope)
                   : CompileColumnWhenNoTableAliasExists(context, node, statementValidationScope);
    }

    Errors::CompilationStatus BindNode(
        const QueryContext &context,
        Expressions::VariableExpression* node,
        const CompilationScope&
    ){
        DataType outType;
        const auto normalizedNameView = DataTypes::StringView::ViewOf(node->normalizedName);
        if (!context._scope.variables.TryGetValue(normalizedNameView, outType)) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_VARIABLE(
                    context.GetAllocator(),
                    node->name
                )
            );
        }

        node->dataType = outType;
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus BindNode(
        const QueryContext &context,
        ColumnName &column,
        CompilationScope& statementValidationScope
    ){
        if (!column.alias.Empty()) {
            UnsignedSmallInt slotIndex;
            if (!statementValidationScope._tableAliasesDict.TryGetValue(column.alias, slotIndex)) {
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_TABLE_ALIAS(
                        context.GetAllocator(),
                        column.alias
                    )
                );
            }

            column._slotIndex = slotIndex;
        }

        bool columnExistsOnTable = false;
        CoreEngine::Catalog::ColumnHeader columnHeader;
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
            return Errors::CompilationStatus::Error(
                Messages::COLUMN_DOES_NOT_EXIST_ON_TABLE(
                    context.GetAllocator(),
                    column.name
                )
            );
        }

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus BindNode(
        const QueryContext&,
        Expressions::ConstantExpression* node,
        CompilationScope&
    ){
        DataTypes::Coercions::DeduceIntegerType(node->value);
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus BindNode(
        QueryContext& context,
        Expressions::JsonExpression* node,
        const CompilationScope&
    ){
        // columnPtr is already bound by the walker
        if (node->pathSegments.Empty())
            return Errors::CompilationStatus::Error(
                Messages::EMPTY_JSON_PATH,
                context.GetAllocator()
            );

        for (auto i = 0; i < node->pathSegments.Size(); i++){
            const auto& segment = node->pathSegments[i];
            if (segment._accessorType == DataTypes::JsonAccessorType::Scalar
                && i != node->pathSegments.Size() - 1
            ) return Errors::CompilationStatus::Error(
                Messages::INVALID_JSON_ACCESSOR_TYPE,
                context.GetAllocator()
            );
        }

        const auto& lastPathSegment = node->pathSegments.Back();
        if (lastPathSegment->_accessorType != DataTypes::JsonAccessorType::Scalar)
            return Errors::CompilationStatus::Error(
                Messages::INVALID_JSON_PATH(
                    context.GetAllocator(),
                    DataTypes::StringView::ViewOf(lastPathSegment->_key)
                )
            );

        //verify json validity maybe
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus CompileColumnWhenTableAliasExists(
        QueryContext& context,
        Expressions::ColumnExpression *column,
        const CompilationScope& statementValidationScope
    ){
        CoreEngine::Catalog::ColumnHeader columnHeader;
        if (!statementValidationScope._tableAliasesDict.TryGetValue(column->tableAlias, column->_slotIndex)) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                    context.GetAllocator(),
                    column->tableAlias
                )
            );
        }

        const auto& columns = statementValidationScope._tableColumnsArray[column->_slotIndex];
        const auto columnAliasToLower = column->alias.ToLower();
        if (!columns.TryGetValue(columnAliasToLower, columnHeader)) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_COLUMN_NAME(
                    context.GetAllocator(),
                    column->alias
                )
            );
        }

        column->columnId = columnHeader.id;
        column->returnType = static_cast<DataType>(columnHeader.dataType);
        column->ordinalPosition = columnHeader.ordinalPosition;
        column->tableId = columnHeader.tableId;

        if (column->name.Empty())
            column->name = columnHeader.name;

        context._referencedColumns.Add(column->_slotIndex, column->ordinalPosition, column->returnType);

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus CompileColumnWhenNoTableAliasExists(
        QueryContext& context,
        Expressions::ColumnExpression *column,
        const CompilationScope& statementValidationScope
    ){
        CoreEngine::Catalog::ColumnHeader columnHeader;
        bool columnExistsOnStatement = false;

        for (Int slotIndex = 0; slotIndex < statementValidationScope._tableColumnsArray.Size();slotIndex++){
            const auto& columns = statementValidationScope._tableColumnsArray[slotIndex];
            const auto columnAliasToLower = column->alias.ToLower();
            if (!columns.TryGetValue(columnAliasToLower, columnHeader))
                continue;

            if (columnExistsOnStatement) {
                return Errors::CompilationStatus::Error(
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
            return Errors::CompilationStatus::Error(
                Messages::INVALID_COLUMN_NAME(
                    context.GetAllocator(),
                    column->alias
                )
            );
        }

        if (column->name.Empty())
            column->name = columnHeader.name;

        context._referencedColumns.Add(column->_slotIndex, column->ordinalPosition, column->returnType);

        return Errors::CompilationStatus::Ok();
    }

    bool ValidateExpressionCoercionTypes(const Expressions::Expression *left, const Expressions::Expression *right){
        // If one side is a column expression, its type takes precedence
        const auto leftType = Expressions::GetExpressionReturnType(left);
        const auto rightType = Expressions::GetExpressionReturnType(right);

        const auto isLeftColumn = left->Is<Expressions::ColumnExpression>();
        const auto isRightColumn = right->Is<Expressions::ColumnExpression>();

        if (isLeftColumn && right->Is<Expressions::ConstantExpression>()) {
            const auto* constant = right->As<Expressions::ConstantExpression>();
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

        if (isRightColumn && left->Is<Expressions::ConstantExpression>()) {
            const auto* constant = left->As<Expressions::ConstantExpression>();

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
        if (expression->Is<Expressions::ConstantExpression>()) {
            auto* constantExpr = expression->As<Expressions::ConstantExpression>();

            if (constantExpr->value.IsNull()
                || DataTypes::Coercions::CanBeParsedToType(type, constantExpr->value)
            ) return true;

            return DataTypes::Coercions::IsCoercionAllowed(constantExpr->GetReturnType(), type);
        }

        return DataTypes::Coercions::IsCoercionAllowed(Expressions::GetExpressionReturnType(expression), type);
    }

    Errors::CompilationStatus CompileWildcard(
        QueryContext& context,
        const Expressions::ColumnExpression* column,
        const CompilationScope& compilationScope,
        SelectStatement* statement
    ){
        if (DataTypes::StringView::ViewOf(column->alias) != WILDCARD)
            return Errors::CompilationStatus::Ok();

        if (compilationScope._indexPos == nullptr)
            return Errors::CompilationStatus::Error(
                Messages::UNEXPECTED_ERROR,
                context.GetAllocator()
            );

        //if no alias is specified get all the columns from the existing tables in the query
        if (column->tableAlias.Empty()) {
            statement->_projections.erase(statement->_projections.begin() + *compilationScope._indexPos);

            for (Int slotIndex = 0; slotIndex < compilationScope._tableColumnsArray.Size(); slotIndex++){
                const auto& columns = compilationScope._tableColumnsArray[slotIndex];
                AssignColumnsFromWildCardExpression(
                    context,
                    columns,
                    column->tableAlias,
                    compilationScope,
                    statement->_projections,
                    slotIndex
                );
                *compilationScope._indexPos += static_cast<Int>(columns.size());
            }

            return Errors::CompilationStatus::Ok();
        }

        //else get only from the specified
        UnsignedSmallInt slotIndex = 0;
        if (!column->tableAlias.Empty()
            && !compilationScope._tableAliasesDict.TryGetValue(column->tableAlias, slotIndex)
        ){
            return Errors::CompilationStatus::Error(
                Messages::INVALID_TABLE_ALIAS(
                    context.GetAllocator(),
                    column->tableAlias
                )
            );
        }

        //remove the wildcard
        statement->_projections.erase(statement->_projections.begin() + *compilationScope._indexPos);
        AssignColumnsFromWildCardExpression(
            context,
            compilationScope._tableColumnsArray[slotIndex],
            column->tableAlias,
            compilationScope,
            statement->_projections,
            slotIndex
        );

        return Errors::CompilationStatus::Ok();
    }

    void AssignColumnsFromWildCardExpression(
        QueryContext& context,
        const Dictionary<DataTypes::String, CoreEngine::Catalog::ColumnHeader>& columnsDict,
        const DataTypes::String& tableAlias,
        const CompilationScope& statementValidationScope,
        DataStructures::PolymorphicArray<Expressions::Expression*>& results,
        const UnsignedSmallInt slotIndex
    ) {
        results.Insert(nullptr, *statementValidationScope._indexPos, static_cast<Int>(columnsDict.size()));
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
            const auto dataType = static_cast<DataType>(header.dataType);
            columnExpression->returnType = dataType;

            const auto insertPos = *statementValidationScope._indexPos + header.ordinalPosition;

            results[insertPos] = columnExpression;
            context._referencedColumns.Add(slotIndex, header.ordinalPosition, dataType);
        }
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext&,
        const Expressions::Expression*,
        Expressions::Expression*&
    ){
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        Expressions::BinaryExpression* node,
        Expressions::Expression*& slot
    ){
        const auto leftType = Expressions::GetExpressionReturnType(node->left);
        const auto rightType = Expressions::GetExpressionReturnType(node->right);
        if (leftType == DataType::Null || rightType == DataType::Null){
            slot = context._compileContext.Allocate<Expressions::ConstantExpression>(Value::Null());
            return Errors::CompilationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(node->left, node->right)) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        if (!node->ValidateOperation()) {
            return Errors::CompilationStatus::Error(
                Messages::INVALID_OPERATION_ON_DATATYPES(
                    context.GetAllocator(),
                    leftType,
                    rightType
                )
            );
        }

        const auto promotedType = PromoteType(leftType, rightType);

        if (leftType != promotedType)
            InsertCastExpression(context, node->left, promotedType);
        if (rightType != promotedType)
            InsertCastExpression(context, node->right, promotedType);

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::LogicalExpression* node,
        Expressions::Expression*&
    ){
        if (!ValidateExpressionCoercionTypes(DataType::Bool, node->left))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(node->left));
        if (!node->IsNot() && !ValidateExpressionCoercionTypes(DataType::Bool, node->right))
            return ClauseCannotBeEvaluatedToBool(context, Expressions::GetExpressionReturnType(node->right));

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::CastExpression* node,
        Expressions::Expression*& slot
    ){
        const auto childReturnType = Expressions::GetExpressionReturnType(node->childExpr);
        if (node->targetType == childReturnType){
            slot = node->childExpr;
            return Errors::CompilationStatus::Ok();
        }

        if (!ValidateExpressionCoercionTypes(node->targetType, node->childExpr)){
            return Errors::CompilationStatus::Error(
                Messages::INVALID_DATATYPE_CONVERSION_MESSAGE(
                    context.GetAllocator(),
                    childReturnType,
                    node->targetType
                )
            );
        }

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        Expressions::BranchExpression* node,
        Expressions::Expression*&
    ){
        for (const auto* branch: node->branches){
            if (!ValidateExpressionCoercionTypes(DataType::Bool, branch)) {
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_EXPRESSION_TYPE_FOR_BRANCH_EXPRESSION(
                        context.GetAllocator(),
                        Expressions::GetExpressionReturnType(branch)
                    )
                );
            }
        }
        const auto promotedType = node->GetReturnType();
        for (auto& resultExpr : node->results) {
            if (!ValidateExpressionCoercionTypes(promotedType, resultExpr))
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                    context.GetAllocator()
                );

            if (promotedType != Expressions::GetExpressionReturnType(resultExpr))
                InsertCastExpression(context, resultExpr, promotedType);
        }

        if (node->HasBaseCase()) {
            if (!ValidateExpressionCoercionTypes(promotedType, node->baseCase))
                return Errors::CompilationStatus::Error(
                    Messages::INVALID_BRANCH_EXPRESSION_RESULT_TYPE,
                    context.GetAllocator()
                );

            if (promotedType != Expressions::GetExpressionReturnType(node->baseCase))
                InsertCastExpression(context, node->baseCase, promotedType);
        }

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::FunctionExpression* node,
        Expressions::Expression*& slot
    ){
        //validate number of arguments
        //TODO
        DataTypes::String errorMessage(context.GetAllocator());
        if (!node->ValidateNumberOfArguments(errorMessage))
            return Errors::CompilationStatus::Error(std::move(errorMessage));

        return Errors::CompilationStatus::Ok();
    }

    // Function nodes fall here too: they have no vectorized kernels yet, and a zero argument call
    // such as GETDATE() would otherwise count as "all arguments constant" and freeze at compile time
    Errors::CompilationStatus FoldNode(
        const QueryContext&,
        const Expressions::Expression*,
        Expressions::Expression*&
    ){
        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::BinaryExpression* node,
        Expressions::Expression *&slot
    ){
        if (IsNullConstant(node->left) || IsNullConstant(node->right))
            slot = TypedNull(context, Expressions::GetExpressionReturnType(node));
        else if (node->left->Is<Expressions::ConstantExpression>()
            && node->right->Is<Expressions::ConstantExpression>()
        ) FoldToConstant(context, slot);

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        Expressions::LogicalExpression* node,
        Expressions::Expression *&slot
    ){
        // NOT has no right operand
        if (node->IsNot()){
            if (node->left->Is<Expressions::ConstantExpression>())
                FoldToConstant(context, slot);
            return Errors::CompilationStatus::Ok();
        }

        const auto dominantValue = node->IsOr();
        if (
            node->left->Is<Expressions::ConstantExpression>()
            && TryPropagateChildExpression(slot, node->left, node->right, dominantValue)
        ) return Errors::CompilationStatus::Ok();

        if (node->right->Is<Expressions::ConstantExpression>())
            TryPropagateChildExpression(slot, node->right, node->left, dominantValue);

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::BranchExpression* node,
        Expressions::Expression *&slot
    ){
        for (Int i = 0;i < node->branches.Size(); i++){
            const auto* branch = node->branches[i];
            if (!branch->Is<Expressions::ConstantExpression>())
                return Errors::CompilationStatus::Ok();

            // a NULL condition counts as false
            const auto& condition = branch->As<Expressions::ConstantExpression>()->value;
            if (!condition.IsNull() && condition.AsBool()) {
                slot = node->results[i];
                return Errors::CompilationStatus::Ok();
            }
        }

        // ternary keeps its else result in results[1], switch in baseCase
        if (node->branchType == Expressions::BranchType::Ternary)
            slot = node->results[1];
        else
            slot = node->HasBaseCase()
                ? node->baseCase
                : TypedNull(context, Expressions::GetExpressionReturnType(node));

        return Errors::CompilationStatus::Ok();
    }

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::CastExpression* node,
        Expressions::Expression *&slot
    ){
        if (node->childExpr->Is<Expressions::ConstantExpression>())
            FoldToConstant(context, slot);

        return Errors::CompilationStatus::Ok();
    }

    void FoldToConstant(const QueryContext& context, Expressions::Expression*& slot){
        Expressions::BindAndResolveExpressionKernel(slot, nullptr);

        const CoreEngine::DataChunk chunk;
        const auto executionContext = CoreEngine::ExecutionContext::BaseContext();

        auto value = Expressions::EvaluateExpressionToValue(slot, &executionContext, &chunk, context.GetAllocator());
        slot = context._compileContext.Allocate<Expressions::ConstantExpression>(value);
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
        Expressions::ForEachColumnReference(expression, [&](Expressions::ColumnExpression* column){
            column->ordinalPosition = columnIndicesDictionary.Get(column->columnId);
        });
    }

    Errors::CompilationStatus ResolveOrderByExpression(
        const QueryContext& context,
        OrderColumn* column,
        const Dictionary<DataTypes::String, Int>& aliasesDictionary,
        const Int visibleProjectionCount
    ){
        auto* expression = column->expression;
        if (expression->Is<Expressions::ColumnExpression>()){
            auto* columnExpr = expression->As<Expressions::ColumnExpression>();
            if (DataTypes::StringView::ViewOf(columnExpr->alias) == WILDCARD)
                return Errors::CompilationStatus::Error(
                    Messages::WILDCARD_NOT_ALLOWED_IN_ORDER_BY,
                    context.GetAllocator()
                );

            if (aliasesDictionary.TryGetValue(columnExpr->alias, column->outputIndex))
                return Errors::CompilationStatus::Ok();


        }

        if (expression->Is<Expressions::ConstantExpression>()){
            const auto* constantExpr = expression->As<Expressions::ConstantExpression>();
            if (constantExpr->value.IsNull())
                return Errors::CompilationStatus::Error(
                    Messages::ORDER_BY_NULL_VALUE,
                    context.GetAllocator()
                );

            if (constantExpr->value.IsIntegral()){
                const auto value = constantExpr->value.AsBigInt();
                if (value < 1 || value > visibleProjectionCount)
                    return Errors::CompilationStatus::Error(
                        Messages::ORDER_BY_INTEGRAL_INVALID_VALUE(
                            context.GetAllocator(),
                            value
                        )
                    );

                column->outputIndex = value - 1;
                return Errors::CompilationStatus::Ok();
            }


        }

    }

    Errors::CompilationStatus CompilePostProjectionColumnExpression(
        const QueryContext& context,
        Expressions::ColumnExpression *column,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    ){
        const Expressions::Expression* expression;
        if (!postProjectionAliases.TryGetValue(column->alias, expression)) {
            return Errors::CompilationStatus::Error(
            Messages::INVALID_COLUMN_NAME(
                        context.GetAllocator(),
                        column->alias
                )
            );
        }

        column->returnType = Expressions::GetExpressionReturnType(expression);
        column->ordinalPosition = expression->ordinalPosition;
        return Errors::CompilationStatus::Ok();
    }

    void AssignPostProjectionIndicesToExpression(
        const Dictionary<DataTypes::String, column_index_t> &columnIndicesDictionary,
        Expressions::Expression *expression
    ){
        Expressions::ForEachColumnReference(expression, [&](Expressions::ColumnExpression* column){
            column->ordinalPosition = columnIndicesDictionary.Get(column->alias);
        });
    }

    Errors::CompilationStatus ClauseCannotBeEvaluatedToBool(const QueryContext& context, const DataType type) {
      return Errors::CompilationStatus::Error(
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

    bool IsNullConstant(const Expressions::Expression* expression){
        if (!expression->Is<Expressions::ConstantExpression>())
            return false;
        return expression->As<Expressions::ConstantExpression>()->value.IsNull();
    }

    Expressions::Expression* TypedNull(const QueryContext& context, const DataType type){
        auto value = Value::Null();
        value.SetType(type);
        return context.GetAllocator()->Allocate<Expressions::ConstantExpression>(value);
    }

    bool TryPropagateChildExpression(
        Expressions::Expression*& expression,
        Expressions::Expression*& leftExpr,
        Expressions::Expression*& rightExpr,
        const bool dominantValue
    ){
        const auto* left = leftExpr->As<Expressions::ConstantExpression>();
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
