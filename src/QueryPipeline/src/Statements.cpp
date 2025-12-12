#include "../include/Statements.h"

#include "../include/Constants.h"
#include "../../Database/include/Database.h"
#include "../../Systemic/include/Coercions.h"
#include "../../Systemic/include/Functions/StringFunctions.h"
#include "../../Server/include/Server.h"
#include "../include/LogicalPlan.h"
#include "../../Server/include/Constants.h"
#include "../../Database/include/SystemDatabases/SystemCatalog.h"

#include <iostream>
#include <ranges>

namespace QueryPipeline::Statements {

  Statement::Statement(){
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->table = nullptr;
    this->server = &Network::Server::Get();
    this->catalog = &DatabaseEngine::SystemCatalog::Get();
  }

  Errors::ValidationStatus Statement::CompileBase()const{
    const auto* session = this->server->GetSession(this->sessionId);

    if (!session || !session->user || !session->user->role)
      return {Errors::ValidationError::Error, "Failed to get user session"};

    if (!session->user->role->HasPermission(this->RequiredPermissions())) {
      ostringstream os;
      os  << "User: " << session->user->name << " is not authorized to perform this action.";

      return {Errors::ValidationError::Error, os.str()};
    }

    return {Errors::ValidationError::Ok, ""};
  }

  Errors::ValidationStatus Statement::Compile(ParserValidationScope& validationScope){
    auto result = this->CompileBase();

    if (!result.IsOk())
      return result;

    return this->CompileDerived(validationScope);
  }

   DeclareVariableStatement::DeclareVariableStatement() {
    this->expression = nullptr;
  }

  Errors::ValidationStatus DeclareVariableStatement::CompileDerived(ParserValidationScope& validationScope) {
    const auto& type = this->variable.GetType();

    if (this->expression) {
      auto res = CompileExpression(validationScope, this->expression);

      if (!res.IsOk())
        return res;

      if (type != DataType::Unknown && !ValidateExpressionCoercionTypes(type, this->expression)) {

        ostringstream os;

        os  << "Cannot convert from: "
            << ColumnTypesToStringDictionary.Get(this->expression->GetReturnType())
            << " to type: " << ColumnTypesToStringDictionary.Get(type)
            << " safely";

        return {Errors::ValidationError::Error, os.str()};
      }

      if (type == DataType::Unknown)
        this->variable.SetType(this->expression->GetReturnType());
    }

    validationScope.variables.ForceAdd(this->variable.GetNormalizedName(), type);

    return {};
  }

  Security::Permission DeclareVariableStatement::RequiredPermissions() const {
    return Constants::DB_WRITER_PERMISSIONS;
  }

  QueryPipeline::LogicalPlan * DeclareVariableStatement::ToLogical() {
    return new LogicalDeclareVariable(this->sessionId, this->variable, this->expression);
  }

  void DeclareVariableStatement::CleanUp() {
    delete this->expression;
  }

   SetVariableStatement::SetVariableStatement() {
    this->expression = nullptr;
  }

  Errors::ValidationStatus SetVariableStatement::CompileDerived(ParserValidationScope& validationScope) {
    const auto& type = this->variable.GetType();

    if (this->expression) {
      auto res = CompileExpression(validationScope, this->expression);

      if (!res.IsOk())
        return res;

      if (type != DataType::Unknown && !ValidateExpressionCoercionTypes(type, this->expression)) {

        ostringstream os;

        os  << "Cannot convert from: "
            << ColumnTypesToStringDictionary.Get(this->expression->GetReturnType())
            << " to type: " << ColumnTypesToStringDictionary.Get(type)
            << " safely";

        return {Errors::ValidationError::Error, os.str()};
      }

      if (type == DataType::Unknown)
        this->variable.SetType(this->expression->GetReturnType());
    }

    validationScope.variables.ForceAdd(this->variable.GetNormalizedName(), type);

    return {};
  }

  Security::Permission SetVariableStatement::RequiredPermissions() const {
    return Constants::DB_WRITER_PERMISSIONS;
  }

  QueryPipeline::LogicalPlan * SetVariableStatement::ToLogical() {
    return new LogicalDeclareVariable(this->sessionId, this->variable, this->expression);
  }

  void SetVariableStatement::CleanUp() {
    delete this->expression;
  }

  Errors::ValidationStatus CreateUserStatement::CompileDerived(ParserValidationScope& validationScope){
    if (this->username.empty())
      return {Errors::ValidationError::Error,  "username cannot be empty"};

    if (this->password.empty())
      return {Errors::ValidationError::Error,  "password cannot be empty"};

    if (this->role.empty())
      return {Errors::ValidationError::Error,  "role cannot be empty"};

    ostringstream os;
    if (this->server->UserExists(this->username)) {
      os << "User with username: " << this->username << " already exists.";

      return {Errors::ValidationError::Error,  os.str()};
    }

    if (!this->server->RoleExists(this->role)) {
      os << "Role: " << this->role << " does not exist.";

      return {Errors::ValidationError::Error,  os.str()};
    }

    return {Errors::ValidationError::Ok,  ""};
  }

  Security::Permission CreateUserStatement::RequiredPermissions() const{
      return Constants::ADMIN_PERMISSIONS;
  }

  LogicalPlan* CreateUserStatement::ToLogical(){
    return new LogicalCreateUser(this->sessionId, this->username, this->password, this->role);
  }

  void CreateUserStatement::CleanUp(){ }

  Errors::ValidationStatus GrantRoleStatement::CompileDerived(ParserValidationScope& validationScope){
    ostringstream os;
    if (!this->server->UserExists(this->username)) {
      os << "User: " << this->username << " does not exist.";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!this->server->RoleExists(this->role)) {
      os << "Role: " << this->role << " does not exist.";
      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  Security::Permission GrantRoleStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

  LogicalPlan * GrantRoleStatement::ToLogical(){
    return new LogicalGrantRole(this->sessionId, this->username, this->role);
  }

  void GrantRoleStatement::CleanUp(){ }

  Errors::ValidationStatus DeleteStatement::CompileDerived(ParserValidationScope& validationScope){
    auto result = this->table->Validate(this->databaseId);

    if (!result.IsOk())
      return result;

    if (this->where.expression == nullptr)
      return {};

    const auto columnsDict = this->catalog->SelectColumnsToDictionary(this->table->tableId);

    return {};
    // return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->table, this->where.expression);
  }

  void DeleteStatement::CleanUp() {
    delete this->table;
    delete this->where.expression;
  }

  Security::Permission DeleteStatement::RequiredPermissions() const{
    return Constants::DB_WRITER_PERMISSIONS;
  }

  JoinStatement::JoinStatement() {
    this->type = Constants::JoinType::Inner;
    this->table = nullptr;
    this->expression = nullptr;
  }

  Errors::ValidationStatus JoinStatement::CompileDerived(ParserValidationScope& validationScope){
    return {};
  }

  Errors::ValidationStatus JoinStatement::Validate(const int32_t& databaseId){
    this->databaseId = databaseId;

    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "No table was specified in the join statement"};

    return this->table->Validate(this->databaseId);
  }

  bool JoinStatement::IsRightJoin() const {
    return this->type == JoinType::Right;
  }

  QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){
    return new LogicalTableScan(this->table, nullptr);
    // return new LogicalJoin(
    //     this->databaseId,
    //     // new LogicalTableScan(),
    //     // this->joinType,
    //     // this->table2,
    //     // this->on.expression
    // );
  }

  Security::Permission JoinStatement::RequiredPermissions() const{
    return Constants::DB_READER_PERMISSIONS;
  }

  void JoinStatement::CleanUp() {
    delete this->table;
    delete this->expression;
  }

  CreateTableStatement::~CreateTableStatement() {
      delete this->constraint;

      for(const auto& column : this->columns)
          delete column;
  }

  // QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){
  //   return new LogicalJoin(this->table, this->joinType, this->table2, this->on.expression);
  // }

  StatementValidationScope::StatementValidationScope(
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary,
    Statement *statement,
    int *indexPos
  ) {
    this->tableAliasesDictionary = &tableAliasesDictionary;
    this->tablesColumnsDictionary = &tablesColumnsDictionary;
    this->statement = statement;
    this->indexPos = indexPos;
  }

  DecimalType::DecimalType(){
    this->precision = Constants::INVALID_DECIMAL_PRECISION;
    this->scale = Constants::INVALID_DECIMAL_SCALE;
  }

  DecimalType::DecimalType(const int8_t &precision, const int8_t &scale){
    this->precision = precision;
    this->scale = scale;
  }

  bool DecimalType::Validate() const{
    if (this->precision == Constants::INVALID_DECIMAL_PRECISION
      || this->scale == Constants::INVALID_DECIMAL_SCALE)
      return false;

    return (
      this->precision <= Constants::MAX_DECIMAL_PRECISION
      && this->scale <= this->precision
    );
  }

  ColumnType::ColumnType(const std::string &name){
    this->name = name;
    this->size = 0;
  }

  ColumnType::ColumnType(const std::string &name, const int &size){
    this->name = name;
    this->size = size;
  }

  ColumnType::ColumnType(const std::string &name, const DecimalType &decimal){
    this->name = name;
    this->decimal = decimal;
    this->size = 0;
  }

  Errors::ValidationStatus Identity::Validate() const{
    if (this->incrementFactor <= 0)
      return {Errors::ValidationError::Error, "Increment Factor must be greater than zero"};

    return {};
  }

  bool NewColumn::HasIdentity()const{ return this->identity != nullptr;}

  OrderColumn::OrderColumn(){
    this->expression = nullptr;
    this->type = OrderType::ASCENDING;
  }

  OrderColumn::~OrderColumn(){
    delete this->expression;
  }

  WhereClause::WhereClause() { this->expression = nullptr; }

  bool WhereClause::IsValid() const{
    return (this->expression == nullptr)
        || this->expression->IsLogical()
        || this->expression->IsBinary();
  }

  OrderByStatement::~OrderByStatement(){
    for (const auto* column: this->columns)
      delete column;
  }

  bool OrderByStatement::Validate(const std::vector<OrderColumn*>& selectColumns, const Dictionary<std::string, Headers::ColumnHeader>& columnsDict){

    HashSet<std::string> selectColumnMap;
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
    //   this->columnIndices.emplace_back(header.ordinalPosition);
    // }

    return true;
  }

  DataSource::DataSource() {
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->tableId = Constants::INVALID_TABLE_ID;
    this->schemaId = Constants::INVALID_SCHEMA_ID;
    this->ordinalPosition = Constants::INVALID_ORDINAL_POS;
    this->schema = "dbo";
    this->server = &Network::Server::Get();
    this->catalog = &DatabaseEngine::SystemCatalog::Get();
  }

  std::string DataSource::GetAlias() const{
    return this->alias.empty()
        ? this->GetFullName()
          : this->alias;
  }

  std::string DataSource::GetFullName() const {
    return (this->database.empty() ? "" : this->database + ".") + this->schema + "." + this->name;
  }

  Errors::ValidationStatus DataSource::Validate(const int32_t& selectedDatabaseId) {
    const auto tableHeader = (!this->database.empty())
        ? this->catalog->SelectTable(this->database, this->name)
        : this->catalog->SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id == Constants::INVALID_TABLE_ID){
      ostringstream os;

      os << "Table " + this->GetFullName() + " does not exist";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->tableId = tableHeader.id;
    this->ordinalPosition = tableHeader.ordinalPosition;
    this->databaseId = tableHeader.databaseId;

    return {};
  }

  Errors::ValidationStatus DataSource::ValidateTableCreate(const int32_t &selectedDatabaseId){
    const auto tableHeader = (!this->database.empty())
      ? this->catalog->SelectTable(this->database, this->name)
      : this->catalog->SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id != Constants::INVALID_TABLE_ID){
      ostringstream os;
      os << "Table " + this->GetFullName() + " exists";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->databaseId = selectedDatabaseId;

    return {};
  }

  CreateTableStatement::CreateTableStatement(){
    this->table = nullptr;
    this->constraint = nullptr;
  }

  Errors::ValidationStatus CreateTableStatement::CompileDerived(ParserValidationScope& validationScope){
    auto result = this->table->ValidateTableCreate(this->databaseId);

    if (!result.IsOk())
      return result;

    ostringstream os;

    const auto& schemasDict = this->catalog->SelectSchemasToDictionary(this->databaseId);
    Headers::SchemaHeader schemaHeader;

    if (!schemasDict.TryGetValue(Functions::String::Lower(this->table->schema), schemaHeader)) {
      os << "Schema: " << this->table->schema << "does not exist.";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->table->schemaId = schemaHeader.id;

    column_index_t tablePosition = 0;
    bool primaryKeyFound = false;
    Dictionary<string, column_index_t> columnNamesToIndexes;

    for (const auto& column: this->columns) {
      uint16_t columnSize;

      if (!ColumnTypeSizes.TryGetValue(column->type.name, columnSize)) {
        os << "Column Type: " + column->type.name + " does not exist";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (columnSize != 0)
        column->type.size = columnSize;

      const auto& dataType = ColumnTypesDictionary.Get(column->type.name);

      if (dataType == DataType::Decimal) {
        if (!column->type.decimal.Validate()) {
          os << "Decimal type requires precision and scale to be set correctly";
          return {Errors::ValidationError::Error, os.str()};
        }

        column->type.size = DataTypes::Decimal::Size(column->type.decimal.precision);
      }

      column->index = tablePosition++;

      columnNamesToIndexes.Add(column->name.name, column->index);

      if(column->isPrimaryKey && primaryKeyFound){
        os << "Cannot have multiple primary keys defined. Consider declaring a composite key";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (column->isPrimaryKey) {
        this->primaryKey.push_back(column->index);
        primaryKeyFound = true;

        //store the pointer if found, else let it be null
        if(column->HasIdentity()) {
          auto identityResult = column->identity->Validate();

          if (!identityResult.IsOk())
            return identityResult;
        }
      }
    }

    if (this->constraint == nullptr)
      return {};

    if (primaryKeyFound) {
      os << "Cannot have a primary key and a constraint declared";
      return {Errors::ValidationError::Error, os.str()};
    }

    //primary key will be clear for sure here
    for (const auto& column: this->constraint->columns)
      this->primaryKey.push_back(columnNamesToIndexes[column.name]);

    return {};
  }

  LogicalPlan * CreateTableStatement::ToLogical(){
    const auto constraintName = this->constraint == nullptr ? "" : this->constraint->name;

    return new LogicalTableCreate(this->sessionId, this->table, this->columns, this->primaryKey, constraintName);
  }

  Security::Permission CreateTableStatement::RequiredPermissions() const{
    return Constants::DB_OWNER_PERMISSIONS;
  }

  void CreateTableStatement::CleanUp() {
    for (auto*& column : this->columns)
      delete column;
  }

  SelectStatement::SelectStatement(){
    this->top = Constants::INVALID_TOP;
    this->distinct = false;
    this->orderBy = nullptr;
  }

  SelectStatement::~SelectStatement(){
      delete this->orderBy;

      for (const auto* join : this->joins) {
        delete join;
      }
  }

  Dictionary<std::string, column_index_t> SelectStatement::CreatePostProjectionIndicesDictionary() const{
    Dictionary<std::string, column_index_t> dict;

    for (int i = 0;i < this->results.size(); i++) {
      const auto& resultExpr = this->results[i];

      if (resultExpr->name.empty())
        continue;

      dict.Add(resultExpr->name, i);
    }

    return dict;
  }

   bool SelectStatement::HasTopStatement() const{ return this->top != Constants::INVALID_TOP; }

  bool SelectStatement::HasJoins()const{ return !this->joins.empty(); }

  bool SelectStatement::HasWhere() const{ return this->where.expression != nullptr; }

  bool SelectStatement::IsConstant() const{ return this->table == nullptr; }

  Errors::ValidationStatus SelectStatement::CompileNoTableStatement(ParserValidationScope& validationScope){
    for (auto& resultExpr : this->results){
      auto exprResult = CompileExpression(validationScope, resultExpr);

      if (!exprResult.IsOk())
        return exprResult;
    }

    if (this->HasJoins())
      return {Errors::ValidationError::Error, "Missing FROM statement but joins were given"};

    return {};
  }

  Errors::ValidationStatus SelectStatement::Compile(ParserValidationScope& validationScope, Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, this->catalog->SelectColumnsToDictionary(this->table->tableId));

    //Add all the join tables to the dictionaries
    for (const auto& join: this->joins) {
      tableAliasesDictionary.Add(join->table->GetAlias(), join->table->tableId);
      this->tableColumnsDictionary.Add(join->table->tableId, this->catalog->SelectColumnsToDictionary(join->table->tableId));
    }

    auto statementValidationScope = StatementValidationScope(
      tableAliasesDictionary,
      this->tableColumnsDictionary,
      this
    );

    //start resolving aliases
    for (int i = 0; i < this->results.size(); i++) {

      statementValidationScope.indexPos = &i;
      auto expressionResult = CompileExpression(validationScope, statementValidationScope, this->results[i]);

      if (!expressionResult.IsOk())
        return expressionResult;
    }

    auto result = this->CompileWhereClause(validationScope, statementValidationScope);
    if (!result.IsOk())
      return result;

    //validate join expressions
    statementValidationScope.indexPos = nullptr;
    for (const auto& join: this->joins) {
      auto expressionResult = CompileExpression(validationScope, statementValidationScope, join->expression);

      if (!expressionResult.IsOk())
        return expressionResult;
    }

    if (this->orderBy == nullptr)
      return {};

    Dictionary<std::string, const Expressions::Expression*> postProjectionAliases;

    for (const auto* resultExpr : this->results) {
      if (resultExpr->name.empty())
        continue;

      if (postProjectionAliases.Contains(resultExpr->name)) {
        ostringstream os;

        os << resultExpr->name << " already exists on result set";
        return {Errors::ValidationError::Error, os.str()};
      }

      postProjectionAliases.Add(resultExpr->name, resultExpr);
    }

    for (const auto& column: this->orderBy->columns) {
      auto postProjectionExpr = CompilePostProjectionExpression(column->expression, postProjectionAliases);

      if (!postProjectionExpr.IsOk())
        return postProjectionExpr;
    }

    return {};
  }

  Errors::ValidationStatus SelectStatement::CompileWhereClause(
    ParserValidationScope &validationScope,
    StatementValidationScope& statementValidationScope
  ) {
    if (!this->HasWhere())
      return {};

    if (!this->where.IsValid())
      return {Errors::ValidationError::Error, "Where expression must be either a logical or a binary expression"};

    auto expressionResult = CompileExpression(validationScope, statementValidationScope, this->where.expression);
    if (!expressionResult.IsOk())
      return expressionResult;

    if (!ValidateExpressionCoercionTypes(DataType::Bool, this->where.expression))
      return ClauseCannotBeEvaluatedToBool(this->where.expression->GetReturnType());

    return {};
  }

  void SelectStatement::AssignColumnsToIndices(const Dictionary<int32_t, column_index_t> &columnIndicesDictionary)const {
    for (const auto& resultExpr : this->results)
      AssignColumnIndicesToExpression(columnIndicesDictionary, resultExpr);

    if (this->where.expression != nullptr)
      AssignColumnIndicesToExpression(columnIndicesDictionary, this->where.expression);

    for (const auto* join : this->joins)
      AssignColumnIndicesToExpression(columnIndicesDictionary, join->expression);
  }

  Errors::ValidationStatus SelectStatement::CompileDerived(ParserValidationScope& validationScope){
    if (!this->joins.empty() && this->table == nullptr) {
      ostringstream os;
      os << "Joins were specified but no calling table was not specified";
      return {Errors::ValidationError::Error, os.str()};
    }

    //resolve expressions here since no column is to be used
    if (this->table == nullptr)
      return this->CompileNoTableStatement(validationScope);

    auto tableResult = this->table->Validate(this->databaseId);
    if (!tableResult.IsOk())
        return tableResult;

    Dictionary<std::string, table_id_t> aliasesDictionary;

    for (const auto& join: this->joins) {
      auto joinResult = join->Validate(this->databaseId);
      if (!joinResult.IsOk())
        return joinResult;
    }

    return this->Compile(validationScope, aliasesDictionary);
  }

  LogicalPlan * SelectStatement::ToLogical(){
    if (this->IsConstant())
      return new LogicalProject(nullptr, this->results, this->columnHeaders);

    LogicalPlan* current = new LogicalTableScan(this->table, !this->HasJoins() ? this->where.expression : nullptr);

    //join re orders take place here
    std::vector<table_id_t> joinOrder;
    joinOrder.reserve(this->joins.size() + 1);

    //needs to re adjust pointers for right join -> left join change.
    joinOrder.push_back(this->table->tableId);
    for (const auto* join : this->joins) {
      if (join->IsRightJoin()) {
        joinOrder.insert(joinOrder.begin(), join->table->tableId);
        continue;
      }

      joinOrder.push_back(join->table->tableId);
    }

    Dictionary<int32_t, column_index_t> columnIndicesDictionary;
    column_index_t columnIndex = 0;

    for (const auto& tableId: joinOrder) {
      //TODO cache them at the beginning
      const auto& columns = this->catalog->SelectColumns(tableId);

      for (const auto &column : columns) {
        if (columnIndicesDictionary.Contains(column.id))
          continue;

        columnIndicesDictionary.Add(column.id, columnIndex + column.ordinalPosition);
      }

      columnIndex += columns.size();
    }

    this->AssignColumnsToIndices(columnIndicesDictionary);

    //here create logical joins with the expressions
    //re order here
    for (const auto& join : this->joins)
      current = new LogicalJoin(current, join->ToLogical(), join->expression, join->type);

    if (this->where.expression != nullptr)
      current = new LogicalFilter(current, this->where.expression);

    const auto postProjectionIndicesDictionary = this->CreatePostProjectionIndicesDictionary();

    current = new LogicalProject(current, this->results, this->columnHeaders);

    if(this->orderBy != nullptr) {
      for (const auto& column : this->orderBy->columns)
        AssignPostProjectionIndicesToExpression(postProjectionIndicesDictionary, column->expression);

      current = new LogicalOrder(current, this->orderBy->columns);
    }

    if (this->distinct)
      current = new LogicalDistinct(current);

    if (this->HasTopStatement())
      current = new LogicalTop(current, this->top);

    return current;
  }

  void SelectStatement::CleanUp() {
    for (auto*& resultExpr : this->results)
      delete resultExpr;

    delete this->table;
    delete this->where.expression;
  }

  Security::Permission SelectStatement::RequiredPermissions() const{
    return Constants::DB_READER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateDbStatement::CompileDerived(ParserValidationScope& validationScope){
    if (this->catalog->DatabaseExists(this->name)) {
      ostringstream os;
      os << "Database " + this->name + " already exists";

      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  LogicalPlan * CreateDbStatement::ToLogical(){
    return new LogicalCreateDatabase(this->sessionId, this->name);
  }

  void CreateDbStatement::CleanUp(){}

  Security::Permission CreateDbStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

   Errors::ValidationStatus DropDbStatement::CompileDerived(ParserValidationScope& validationScope){
    ostringstream os;

    const auto database = this->catalog->SelectDatabase(this->name);

    if (database.name.empty()) {
      os << "Cannot drop: " << this->name << ". Database" << this->name << " does not exist";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (database.isSystem) {
      os << "Cannot drop: " << this->name << ". Database" << this->name << " is a system database";
      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }
  
  LogicalPlan * DropDbStatement::ToLogical(){
    return nullptr;
  }

  Security::Permission DropDbStatement::RequiredPermissions() const{
    return Constants::ADMIN_PERMISSIONS;
  }

  void DropDbStatement::CleanUp(){ }

  Errors::ValidationStatus UseDatabaseStatement::CompileDerived(ParserValidationScope& validationScope){
    const auto dbHeader = this->catalog->SelectDatabase(this->name);

    if (dbHeader.id == Constants::INVALID_DATABASE_ID) {
      ostringstream os;

      os << "Database " + this->name + " does not exist";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->databaseId = dbHeader.id;

    return {};
  }

  LogicalPlan * UseDatabaseStatement::ToLogical(){
    return new LogicalUseDatabase(this->sessionId, this->databaseId);
  }

  void UseDatabaseStatement::CleanUp(){}

  Security::Permission UseDatabaseStatement::RequiredPermissions() const{
    return Constants::GUEST_PERMISSIONS;
  }

   InsertStatement::~InsertStatement(){
    delete this->selectStatement;
  }

  void InsertStatement::InsertDefaultValuesForMissingColumns(const Headers::ColumnHeader &header, const Headers::DefaultValuesHeader& defaultValue){
    this->columns.emplace_back(ColumnName{
      .name = header.name,
      .alias = header.name,
      .tableId = this->table->tableId,
      .columnId = header.id,
      .index = static_cast<column_index_t>(header.ordinalPosition),
      .returnType = static_cast<DataType>(header.dataType),
    });

    //Insert the default value
    for (auto& [insertColumns] : this->values) {
      const auto* data = reinterpret_cast<const unsigned char*>(defaultValue.value.data());

      insertColumns.emplace_back(
          new Expressions::ConstantExpression(Value(
            data,
            static_cast<int>(defaultValue.value.size()),
            static_cast<DataType>(header.dataType)
          )));
    }
  }

  void InsertStatement::InsertNullValuesForMissingColumns(const Headers::ColumnHeader& header){
    this->columns.emplace_back(ColumnName{
      .name = header.name,
      .alias = header.name,
      .tableId = this->table->tableId,
      .columnId = header.id,
      .index = static_cast<column_index_t>(header.ordinalPosition),
      .returnType = static_cast<DataType>(header.dataType),
    });

    for (auto& [insertColumns] : this->values)
      insertColumns.emplace_back(new Expressions::ConstantExpression(Value(nullptr, header.ordinalPosition)));
  }

  Errors::ValidationStatus InsertStatement::ValidateReturnType(const Expressions::Expression* expression, const std::string& columnName) const{
    const auto valueType = expression->GetReturnType();

    const auto& columnsDictionary = this->tableColumnsDictionary.Get(this->table->tableId);

    const auto& columnHeader = columnsDictionary.Get(Functions::String::Lower(columnName));

    const auto columnType = static_cast<DataType>(columnHeader.dataType);

    if (DataTypes::Coercions::IsCoercionAllowed(
      valueType,
        columnType
      ))
      return {};

    ostringstream os;

    if (expression->IsConstant()) {
      auto* constantExpr = expression->AsConstant();

      if (constantExpr->value.IsNull()) {
        if (columnHeader.isNullable)
          return {};


        os << "Column " << columnHeader.name << " does not allow NULL. Insert fails.";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (DataTypes::Coercions::CanBeParsedToType(columnType, constantExpr->value))
        return {};
    }

    os << "Cannot update column " << columnHeader.name << " of type "
              << ColumnTypesToStringDictionary.Get(columnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType);

    return {Errors::ValidationError::Error, os.str()};
  }

  bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

  Errors::ValidationStatus InsertStatement::ValidateSelectStatement(ParserValidationScope& validationScope)const{

    if (this->selectStatement == nullptr)
      return {};

    if (this->selectStatement->results.size() != this->columns.size())
      return {Errors::ValidationError::Error, "Invalid number of arguments specified on select statement"};

    this->selectStatement->databaseId = this->databaseId;

    auto selectStatus = this->selectStatement->CompileDerived(validationScope);
    if (!selectStatus.IsOk())
      return selectStatus;

    for (int i = 0;i < this->selectStatement->results.size();i++) {
      const auto& resultExpression = this->selectStatement->results[i];

      auto returnTypeStatus = this->ValidateReturnType(resultExpression, this->columns[i].name);
      if (!returnTypeStatus.IsOk())
        return returnTypeStatus;
    }

    return {};
  }

  Errors::ValidationStatus InsertStatement::ResolveAliases(ParserValidationScope& validationScope){
    Dictionary<std::string, table_id_t> tableAliasesDictionary{
      {this->table->GetAlias(), this->table->tableId}
    };

    auto statementValidationScope = StatementValidationScope(
      tableAliasesDictionary,
      this->tableColumnsDictionary,
      this,
      nullptr
    );

    for (auto& [insertColumns] : this->values) {

      for (int i = 0;i < insertColumns.size(); i++) {
        auto& value = insertColumns[i];

        auto expressionStatus = CompileExpression(validationScope, statementValidationScope, value);
        if (!expressionStatus.IsOk())
          return expressionStatus;

        auto returnTypeStatus = this->ValidateReturnType(value, this->columns[i].name);
        if (!returnTypeStatus.IsOk())
          return returnTypeStatus;
      }
    }

    return {};
  }
  //TODO validate length of columns to match max record_size from master DB
  Errors::ValidationStatus InsertStatement::CompileDerived(ParserValidationScope& validationScope){
    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "No table was specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = this->catalog->SelectColumnsToDictionary(this->table->tableId);

    this->tableColumnsDictionary.Add(this->table->tableId, columnsDict);

    const auto identityColumns = this->catalog->SelectIdentityColumnsByTableIdToDictionary(this->table->tableId);

    //validate insert columns existance
    HashSet<int32_t> statementColumns;
    ostringstream os;
    for (auto & column : this->columns) {
      Headers::ColumnHeader header;

      //check if columns exist on the table
      if (!columnsDict.TryGetValue(Functions::String::Lower(column.name), header)) {
        os << "Column " << column.name << " does not exist on table: " << this->table->GetFullName();
        return {Errors::ValidationError::Error, os.str()};
      }

      //check if the specified column is an identity column
      if (identityColumns.Contains(header.id)) {
        os << "Cannot specify an identity column for insert";
        return {Errors::ValidationError::Error, os.str()};
      }

      column.index = header.ordinalPosition;
      column.columnId = header.id;
      statementColumns.Add(header.id);

      this->columnIndices.emplace_back(header.ordinalPosition);
    }
    
    for (const auto&[columnName, header]:  columnsDict) {
      if (header.isSystem
        || identityColumns.Contains(header.id)
        || statementColumns.Contains(header.id))
        continue;

      this->columnIndices.emplace_back(static_cast<column_index_t>(header.ordinalPosition));

      //Insert the null value
      if (header.isNullable) {
        this->InsertNullValuesForMissingColumns(header);
        continue;
      }

      const auto defaultValue = this->catalog->SelectDefaultValueByColumnId(header.id);

      if (defaultValue.columnId == Constants::INVALID_COLUMN_ID) {
        os << "Column " << columnName << " does not allow NULLS. Insert fails";
        return {Errors::ValidationError::Error, os.str()};
      }

      this->InsertDefaultValuesForMissingColumns(header, defaultValue);
    }

    return (this->HasSelectStatement())
      ? this->ValidateSelectStatement(validationScope)
      : this->ResolveAliases(validationScope);
  }

  LogicalPlan* InsertStatement::ToLogical() {

    auto* logicalSelect = this->HasSelectStatement()
        ? this->selectStatement->ToLogical()
        : nullptr;

    return new QueryPipeline::LogicalInsert(this->table, this->values, logicalSelect, this->columnIndices);
  }

  void InsertStatement::CleanUp() {
    delete this->table;
  }

  Security::Permission InsertStatement::RequiredPermissions() const{
    return Constants::DB_WRITER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateSchemaStatement::CompileDerived(ParserValidationScope& validationScope){
    if (this->catalog->SchemaExists(this->databaseId, this->name)) {
      ostringstream os;
      os << "Schema " << this->name << " already exists";

      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  QueryPipeline::LogicalPlan * CreateSchemaStatement::ToLogical(){
    return new LogicalSchemaCreate(this->sessionId, this->databaseId, this->name);
  }

  Security::Permission CreateSchemaStatement::RequiredPermissions() const{
    return Constants::DB_OWNER_PERMISSIONS;
  }

  void CreateSchemaStatement::CleanUp(){}

  UpdateColumn::UpdateColumn(){
    this->value = nullptr;
    this->name = {};
  }

  UpdateColumn::~UpdateColumn(){
    delete this->value;
  }

  Errors::ValidationStatus UpdateStatement::ValidateReturnType(const UpdateColumn* update) const{
    ostringstream os;

    const auto valueType = update->value->GetReturnType();

    if (DataTypes::Coercions::IsCoercionAllowed(
      valueType,
        update->name.returnType)
        )
      return {};

    if (update->value->IsConstant()) {
      const auto* constantExpr = update->value->AsConstant();

      if (constantExpr->value.IsNull()) {
        const auto& columns = this->tableColumnsDictionary.Get(this->table->tableId);

        if (columns.Get(update->name.name).isNullable)
          return {};

        os << "Column " << update->name.name << " does not allow NULL. Update fails.";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (DataTypes::Coercions::CanBeParsedToType(update->name.returnType, constantExpr->value))
        return {};
    }

    os << "Cannot update column " << update->name.name << " of type "
              << ColumnTypesToStringDictionary.Get(update->name.returnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType);

    return {Errors::ValidationError::Error, os.str()};
  }

  Errors::ValidationStatus UpdateStatement::ResolveAliases(ParserValidationScope& validationScope, Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, this->catalog->SelectColumnsToDictionary(this->table->tableId));

    auto statementValidationScope = StatementValidationScope(
      tableAliasesDictionary,
      this->tableColumnsDictionary,
      this,
      nullptr
    );

    //start resolving aliases
    for (const auto& update : this->updates) {
      auto columnAliasStatus = CompileColumnExpression(update->name, statementValidationScope);
      if (!columnAliasStatus.IsOk())
        return columnAliasStatus;

      auto expressionStatus = CompileExpression(validationScope, statementValidationScope, update->value);
      if (!expressionStatus.IsOk())
          return expressionStatus;

      auto returnTypeResult = this->ValidateReturnType(update);
      if (!returnTypeResult.IsOk())
        return returnTypeResult;
    }

    //validate all expressions are valid
    if (this->where.expression != nullptr) {
      if (!this->where.IsValid())
        return {Errors::ValidationError::Error, "Where expression must be either a logical or a binary expression"};

      auto expressionStatus = CompileExpression(validationScope, statementValidationScope, this->where.expression);
      if (!expressionStatus.IsOk())
        return expressionStatus;
    }

    return {};
  }


Errors::ValidationStatus UpdateStatement::CompileDerived(ParserValidationScope& validationScope){

    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "Table was not specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    Dictionary<std::string, table_id_t> aliasesDictionary;
    return this->ResolveAliases(validationScope, aliasesDictionary);
  }

  QueryPipeline::LogicalPlan* UpdateStatement::ToLogical(){
    return new QueryPipeline::LogicalUpdate(this->table, this->updates, this->where.expression);
  }

  void UpdateStatement::CleanUp() {
    for (auto*& update : this->updates)
      delete update;

    delete this->table;
    delete this->where.expression;
  }

  Security::Permission UpdateStatement::RequiredPermissions() const{
    return Constants::DB_WRITER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateIndexStatement::CompileDerived(ParserValidationScope& validationScope){
    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = this->catalog->SelectColumnsToDictionary(this->table->tableId);

    ostringstream os;
    for(auto& column: this->columns) {
      Headers::ColumnHeader header;

      if (columnsDict.TryGetValue(column, header)) {
        this->columnIndices.push_back(header.ordinalPosition);
        continue;
      }

      os << "Column " << column << " does not exist on table: " << this->table->schema << "." << this->table->name;
      return {Errors::ValidationError::Error, os.str()};
    }

    const auto indexes = this->catalog->SelectIndexes(this->table->tableId);

    for (const auto& index: indexes) {
      const auto indexedColumns = this->catalog->SelectIndexColumnsByIndexIdToDictionary(index.id);

      if (index.name == this->name) {
        os << "Index with name: " << index.name << " already exists";
        return {Errors::ValidationError::Error, os.str()};
      }

      //check if identical index exists (no need for a duplicate).
    }

    return {};
  }

  QueryPipeline::LogicalPlan * CreateIndexStatement::ToLogical(){
    return new QueryPipeline::LogicalIndexCreate(this->sessionId, this->table, this->name, this->columnIndices);
  }

  void CreateIndexStatement::CleanUp() {
    delete this->table;
  }

  Security::Permission CreateIndexStatement::RequiredPermissions() const{
    return Constants::DB_OWNER_PERMISSIONS;
  }

  Errors::ValidationStatus AlterTableStatement::CompileAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    ostringstream os;

    auto* newColumn = this->column.newColumn;

    if (headers.Contains(Functions::String::NormalizeString(newColumn->name.name))) {
      os << "Column " << newColumn->name.name << " already exists on table: "<< this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!newColumn->isNullable
      && newColumn->defaultValue.IsNull()) {
      os << "Cannot insert default Value NULL when NOT NULL is specified";
      return {Errors::ValidationError::Error, os.str()};
    }

    newColumn->index = headers.size();

    DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(Functions::String::NormalizeString(newColumn->type.name), columnType)) {
      os << "Invalid Column Type " << newColumn->type.name;
      return {Errors::ValidationError::Error, os.str()};
    }

    const auto recordSize = ColumnTypeSizes.Get(newColumn->type.name);

    if (recordSize != 0)
      newColumn->type.size = recordSize;

    if (columnType == DataType::Decimal) {
      if (!newColumn->type.decimal.Validate()) {
        os << "Decimal type requires precision and scale to be set correctly";
        return {Errors::ValidationError::Error, os.str()};
      }

      newColumn->type.size = DataTypes::Decimal::Size(newColumn->type.decimal.precision);
    }

    //TODO Check this
    // this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

    return {};
  }

  Errors::ValidationStatus AlterTableStatement::CompileAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    ostringstream os;

    auto* alterColumn = this->column.alterColumn;

    if (!headers.TryGetValue(Functions::String::NormalizeString(alterColumn->name.name), header)) {
      os << "Column " << alterColumn->name.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(Functions::String::NormalizeString(alterColumn->type.name), columnType)) {
      os << "Invalid Column Type " << alterColumn->type.name;
      return {Errors::ValidationError::Error, os.str()};
    }

    if ((PipelineConstants::ValidTableStringConversions.Contains(columnType)
      && !PipelineConstants::ValidTableStringConversions.Contains(static_cast<DataType>(header.dataType)))
      || (PipelineConstants::ValidTableIntegerConversions.Contains(columnType)
        && !PipelineConstants::ValidTableIntegerConversions.Contains(static_cast<DataType>(header.dataType)))){
          os << "Cannot alter column " << alterColumn->name.name << " from type: "
                    << ColumnTypesToStringDictionary.Get(static_cast<DataType>(header.dataType))
                    << "to type: " << alterColumn->type.name;

        return {Errors::ValidationError::Error, os.str()};
    }

    if (header.recordSize > alterColumn->type.size) {
      os << "Cannot alter column " << alterColumn->type.name
                << " with size " <<  header.recordSize << " to size: " << alterColumn->type.size
                << std::endl
                << "Use FORCE if potential data corruption is acceptable";
      return {Errors::ValidationError::Error, os.str()};
    }

    alterColumn->columnId = header.id;
    return {};
  }

  Errors::ValidationStatus AlterTableStatement::CompileDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    ostringstream os;
    Headers::ColumnHeader header;

    auto* dropColumn = this->column.dropColumn;

    if (!headers.TryGetValue(Functions::String::NormalizeString(dropColumn->name.name), header)) {
      os << "Column " << dropColumn->name.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    //validate no index or constraint uses it
    const auto constraints = this->catalog->SelectConstraints(this->table->tableId);

    for (const auto& constraint: constraints) {
      const auto columns = this->catalog->SelectConstraintColumnsByConstraintIdToDictionary(constraint.constraintId);

      if (columns.Contains(header.id)) {
        os << "Cannot drop column: " << header.name << " as it is referenced by constraint: " << constraint.name;
        return {Errors::ValidationError::Error, os.str()};
      }
    }

    dropColumn->index = header.ordinalPosition;
    return {};
  }

  Errors::ValidationStatus AlterTableStatement::CompileRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;

    auto* renameColumn = this->column.renameColumn;

    if (!headers.TryGetValue(Functions::String::NormalizeString(renameColumn->oldName.name), header)) {
      ostringstream os;
      os << "Column " << renameColumn->oldName.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};;
    }

    renameColumn->columnId = header.id;
    renameColumn->ordinalPosition = header.ordinalPosition;

    return {};
  }

  Errors::ValidationStatus AlterTableStatement::CompileDerived(ParserValidationScope& validationScope){
    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "Table was not specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = this->catalog->SelectColumnsToDictionary(this->table->tableId);

    //validate by type
    switch (this->type) {
      case AlterTableType::AddColumn:
        return this->CompileAddColumn(columnsDict);
      case AlterTableType::AlterColumn:
        return this->CompileAlterColumn(columnsDict);
      case AlterTableType::DropColumn:
        return this->CompileDropColumn(columnsDict);
      case AlterTableType::RenameColumn:
        return this->CompileRenameColumn(columnsDict);
      default:
        return {Errors::ValidationError::Error, "Unknown table type"};
    }
  }

  QueryPipeline::LogicalPlan * AlterTableStatement::ToLogical(){
    switch (this->type) {
    case AlterTableType::AddColumn:
      return new LogicalAlterTable(this->sessionId, this->table, this->type,this->column.newColumn);
    case AlterTableType::AlterColumn:
      return new LogicalAlterTable(this->sessionId, this->table, this->type,this->column.alterColumn);
    case AlterTableType::RenameColumn:
      return new LogicalAlterTable(this->sessionId, this->table, this->type,this->column.renameColumn);
    case AlterTableType::DropColumn:
      return new LogicalAlterTable(this->sessionId, this->table, this->type,this->column.dropColumn);
    default:
      return nullptr;
    }
  }

  void AlterTableStatement::CleanUp() {
    delete this->column.newColumn;
    delete this->column.dropColumn;
    delete this->column.alterColumn;
    delete this->column.renameColumn;
    delete this->table;
  }

  Security::Permission AlterTableStatement::RequiredPermissions() const{
    return Constants::DB_OWNER_PERMISSIONS;
  }

  Errors::ValidationStatus CompileExpression(ParserValidationScope& validationScope, Expressions::Expression*& expression){
    switch (expression->expressionType) {
      case Expressions::ExpressionType::Binary:
        return CompileBinaryExpression(validationScope, expression->AsBinary(), expression);
      case Expressions::ExpressionType::Logical:
        return CompileLogicalExpression(validationScope, expression->AsLogical(), expression);
      case Expressions::ExpressionType::Branch:
        return CompileBranchExpression(validationScope, expression->AsBranch(), expression);
      case Expressions::ExpressionType::Function:
        return CompileFunctionExpression(validationScope, expression->AsFunction(), expression);
      case Expressions::ExpressionType::Column:
        return CompileColumnExpression(expression->AsColumn());
      case Expressions::ExpressionType::Variable:
        return CompileVariableExpression(validationScope, expression->AsVariable());
      case Expressions::ExpressionType::Constant:
        return CompileConstantExpression(expression->AsConstant());
      case Expressions::ExpressionType::Expression:
      default:
        break;
    }

    return {Errors::ValidationError::Error, "Unknown expression"};
  }

  Errors::ValidationStatus CompileExpression(
      ParserValidationScope& validationScope,
      StatementValidationScope& statementValidationScope,
      Expressions::Expression*& expression
    ){

    switch (expression->expressionType) {
      case Expressions::ExpressionType::Binary:
        return CompileBinaryExpression(validationScope, expression->AsBinary(), expression, statementValidationScope);
      case Expressions::ExpressionType::Logical:
        return CompileLogicalExpression(validationScope, expression->AsLogical(), expression, statementValidationScope);
      case Expressions::ExpressionType::Branch:
        return CompileBranchExpression(validationScope, expression->AsBranch(), expression, statementValidationScope);
      case Expressions::ExpressionType::Function:
        return CompileFunctionExpression(validationScope, expression->AsFunction(), expression, statementValidationScope);
      case Expressions::ExpressionType::Column:
        return CompileColumnExpression(expression->AsColumn(), statementValidationScope);
      case Expressions::ExpressionType::Variable:
        return CompileVariableExpression(validationScope, expression->AsVariable());
      case Expressions::ExpressionType::Constant:
        return CompileConstantExpression(expression->AsConstant());
      case Expressions::ExpressionType::Expression:
      default:
        break;
    }

    return {Errors::ValidationError::Error, "Unknown expression"};
  }

  Errors::ValidationStatus CompileBinaryExpression(
    ParserValidationScope& validationScope,
    Expressions::BinaryExpression *binaryExpr,
    Expressions::Expression *&expression
  ) {
    auto result = CompileExpression(validationScope, binaryExpr->left)
                && CompileExpression(validationScope, binaryExpr->right);

    if (!result.IsOk())
      return result;

    //validate binary expression action
    if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
      ostringstream os;

      const auto& leftTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->left->GetReturnType());
      const auto& rightTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->right->GetReturnType());

      os << "Invalid conversion between " << leftTypeStr << "and " << rightTypeStr <<".Use explicit cast";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!binaryExpr->ValidateOperation()) {
      ostringstream os;
      os  << "Invalid operation between datatypes: "
          << ColumnTypesToStringDictionary.Get(binaryExpr->left->GetReturnType())
          << " and "
          << ColumnTypesToStringDictionary.Get(binaryExpr->right->GetReturnType());

      return {Errors::ValidationError::Error, os.str()};
    }

    FoldExpression(binaryExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileBinaryExpression(
    ParserValidationScope& validationScope,
    Expressions::BinaryExpression* binaryExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  ) {
    auto result = CompileExpression(validationScope, statementValidationScope, binaryExpr->left)
          && CompileExpression(validationScope, statementValidationScope, binaryExpr->right);

    if (!result.IsOk())
      return result;

    if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
      ostringstream os;

      const auto& leftTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->left->GetReturnType());
      const auto& rightTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->right->GetReturnType());

      os << "Invalid conversion between " << leftTypeStr << "and " << rightTypeStr <<".Use explicit cast";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!binaryExpr->ValidateOperation()) {
      ostringstream os;
      os  << "Invalid operation between datatypes: "
          << ColumnTypesToStringDictionary.Get(binaryExpr->left->GetReturnType())
          << " and "
          << ColumnTypesToStringDictionary.Get(binaryExpr->right->GetReturnType());

      return {Errors::ValidationError::Error, os.str()};
    }

    FoldExpression(binaryExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileLogicalExpression(
    ParserValidationScope &validationScope,
    Expressions::LogicalExpression *logicalExpr,
    Expressions::Expression *&expression
  ) {
    //validate type
    auto result = CompileExpression(validationScope, logicalExpr->left)
            && CompileExpression(validationScope, logicalExpr->right);

    if (!result.IsOk())
      return result;

    if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->left))
      return ClauseCannotBeEvaluatedToBool(logicalExpr->left->GetReturnType());
    if (ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
      return ClauseCannotBeEvaluatedToBool( logicalExpr->right->GetReturnType());

    FoldExpression(logicalExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileLogicalExpression(
      ParserValidationScope& validationScope,
      Expressions::LogicalExpression* logicalExpr,
      Expressions::Expression*& expression,
      StatementValidationScope& statementValidationScope
    ) {
    auto result = CompileExpression(validationScope, statementValidationScope, logicalExpr->left)
                    && CompileExpression(validationScope, statementValidationScope, logicalExpr->right);

    if (!result.IsOk())
      return result;

    if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->left))
      return ClauseCannotBeEvaluatedToBool(logicalExpr->left->GetReturnType());
    if (!ValidateExpressionCoercionTypes(DataType::Bool, logicalExpr->right))
      return ClauseCannotBeEvaluatedToBool( logicalExpr->right->GetReturnType());

    FoldExpression(logicalExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileFunctionExpression(
    ParserValidationScope &validationScope,
    const Expressions::FunctionExpression *funcExpr,
    Expressions::Expression *&expression
  ) {
    for (auto* childExpr : funcExpr->arguments) {
      auto childExpressionResult = CompileExpression(validationScope, childExpr);

      if (!childExpressionResult.IsOk())
        return childExpressionResult;
    }

    //validate functionExpression
    std::string errorMessage;
    if (!funcExpr->ValidateNumberOfArguments(errorMessage))
      return {Errors::ValidationError::Error, errorMessage};

    FoldExpression(funcExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileFunctionExpression(
    ParserValidationScope &validationScope,
    const Expressions::FunctionExpression *funcExpr,
    Expressions::Expression *&expression,
    StatementValidationScope& statementValidationScope
  ) {
    //validate children expressions and assign return types and ids to column expressions
    for (auto* childExpr : funcExpr->arguments) {
      auto childExpressionResult = CompileExpression(validationScope, statementValidationScope, childExpr);

      if (!childExpressionResult.IsOk())
        return childExpressionResult;
    }

    //validate number of arguments
    std::string errorMessage;
    if (!funcExpr->ValidateNumberOfArguments(errorMessage))
      return {Errors::ValidationError::Error, errorMessage};

    FoldExpression(funcExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileBranchExpression(
    ParserValidationScope &validationScope,
    Expressions::BranchExpression *branchExpr,
    Expressions::Expression *&expression,
    StatementValidationScope& statementValidationScope
  ) {
    if (!branchExpr->ValidateNumberOfArguments())
      return {Errors::ValidationError::Error, "Invalid number of arguments specified on branching expression"};

    for (auto& branch : branchExpr->branches) {
      auto result = CompileExpression(validationScope, statementValidationScope, branch);
      if (!result.IsOk())
        return result;

      if (!ValidateExpressionCoercionTypes(DataType::Bool, branch)) {
        ostringstream os;

        os  << "Expression of type: "
            << ColumnTypesToStringDictionary.Get(branch->GetReturnType())
            << " cannot be used as a branching condition";

        return {Errors::ValidationError::Error, os.str()};
      }
    }

    for (auto& resultExpr : branchExpr->results) {
      auto result = CompileExpression(validationScope, statementValidationScope, resultExpr);
      if (!result.IsOk())
        return result;
    }

    if (branchExpr->HasBaseCase()) {
      auto result = CompileExpression(validationScope, statementValidationScope, branchExpr->baseCase);
      if (!result.IsOk())
        return result;
    }

    const auto returnType = branchExpr->GetReturnType();

    for (const auto& resultExpr : branchExpr->results) {
      if (!ValidateExpressionCoercionTypes(returnType, resultExpr))
        return {Errors::ValidationError::Error, "Branching Expression Result types cannot be coerced to datatype"};
    }

    if (branchExpr->HasBaseCase()) {
      if (!ValidateExpressionCoercionTypes(returnType, branchExpr->baseCase))
        return {Errors::ValidationError::Error, "Branching Expression Result types cannot be coerced to datatype"};
    }

    FoldExpression(branchExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileBranchExpression(
    ParserValidationScope &validationScope,
    Expressions::BranchExpression *branchExpr,
    Expressions::Expression *&expression
  ){
    if (!branchExpr->ValidateNumberOfArguments())
      return {Errors::ValidationError::Error, "Invalid number of arguments specified on branching expression"};

    for (auto& branch : branchExpr->branches) {
      auto result = CompileExpression(validationScope, branch);
      if (!result.IsOk())
        return result;
    }

    for (auto& resultExpr : branchExpr->results) {
      auto result = CompileExpression(validationScope, resultExpr);
      if (!result.IsOk())
        return result;
    }

    if (branchExpr->HasBaseCase()) {
      auto result = CompileExpression(validationScope, branchExpr->baseCase);
      if (!result.IsOk())
        return result;
    }

    const auto returnType = branchExpr->GetReturnType();

    for (const auto& resultExpr : branchExpr->results) {
      if (!ValidateExpressionCoercionTypes(returnType, resultExpr))
        return {Errors::ValidationError::Error, "Branching Expression Result types cannot be coerced to datatype"};
    }

    if (branchExpr->HasBaseCase()) {
      if (!ValidateExpressionCoercionTypes(returnType, branchExpr->baseCase))
        return {Errors::ValidationError::Error, "Branching Expression Result types cannot be coerced to datatype"};
    }

    FoldExpression(branchExpr, expression);
    return {};
  }

  Errors::ValidationStatus CompileColumnExpression(const Expressions::ColumnExpression *columnExpr){
      ostringstream os;

      os << "No table was specified but column with name: " << columnExpr->alias << " was specified.";

      return {Errors::ValidationError::Error, os.str()};
}

  Errors::ValidationStatus CompileColumnExpression(
      Expressions::ColumnExpression* column,
      const StatementValidationScope& statementValidationScope
    ){
        //if wildcard ensure statement is of select statement type
        if (column->alias == Constants::WILDCARD) {
          auto* selectStatement = dynamic_cast<SelectStatement*>(statementValidationScope.statement);

          if (selectStatement != nullptr)
            return CompileWildcard(column, statementValidationScope, selectStatement);

          return {Errors::ValidationError::Error, ""};
        }

       return column->HasTableAlias()
            ? CompileColumnWhenTableAliasExists(column, statementValidationScope)
            : CompileColumnWhenNoTableAliasExists(column, statementValidationScope);
  }

  Errors::ValidationStatus CompileVariableExpression(
    const ParserValidationScope &validationScope,
    Expressions::VariableExpression* variableExpr
  ){
    DataType type;
    if (!validationScope.variables.TryGetValue(variableExpr->normalizedName, type)) {
      ostringstream os;
      os << "Variable: " << variableExpr->name <<" was not declared in this scope";
      return {Errors::ValidationError::Error, os.str()};
    }

    variableExpr->dataType = type;

    return {};
  }

  Errors::ValidationStatus CompileColumnExpression(
    ColumnName &column,
    StatementValidationScope& statementValidationScope
  ){
    ostringstream os;
    if (!column.alias.empty()) {
      table_id_t tableId;

      if (!statementValidationScope.tableAliasesDictionary->TryGetValue(column.alias, tableId)) {
        os << "Alias: " << column.alias << " does not exist in the statement";
        return {Errors::ValidationError::Error, os.str()};
      }

      column.tableId = tableId;
    }

    bool columnExistsOnTable = false;
    Headers::ColumnHeader columnHeader;
    for (const auto& [key, columns]: *statementValidationScope.tablesColumnsDictionary) {
      if (!columns.TryGetValue(Functions::String::Lower(column.name), columnHeader))
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
      os << "Column: " << column.name << " does not exist on Table";
      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  Errors::ValidationStatus CompileConstantExpression(Expressions::ConstantExpression* literalExpr){
      DataTypes::Coercions::DeduceIntegerType(literalExpr->value);
      return {};
  }

  Errors::ValidationStatus CompileColumnWhenTableAliasExists(
    Expressions::ColumnExpression *column,
    const StatementValidationScope& statementValidationScope
  ){
    ostringstream os;

    table_id_t tableId;
    Headers::ColumnHeader columnHeader;

    if (!statementValidationScope.tableAliasesDictionary->TryGetValue(column->tableAlias, tableId)) {
      os << "Alias: " << column->tableAlias << " does not exist in the statement";
      return {Errors::ValidationError::Error, os.str()};
    }

    column->tableId = tableId;

    const auto& columns = statementValidationScope.tablesColumnsDictionary->Get(column->tableId);

    if (!columns.TryGetValue(Functions::String::Lower(column->alias), columnHeader)) {
      os << "column: " << column->alias << " does not exist in the statement";
      return {Errors::ValidationError::Error, os.str()};
    }

    column->columnId = columnHeader.id;
    column->returnType = static_cast<DataType>(columnHeader.dataType);
    column->index = columnHeader.ordinalPosition;

    if (column->name.empty())
      column->name = columnHeader.name;

    return {};
  }

  Errors::ValidationStatus CompileColumnWhenNoTableAliasExists(
    Expressions::ColumnExpression *column,
    const StatementValidationScope& statementValidationScope
  ){
    ostringstream os;

    Headers::ColumnHeader columnHeader;
    bool columnExistsOnStatement = false;

    for (const auto &columns : *statementValidationScope.tablesColumnsDictionary | views::values) {
      if (!columns.TryGetValue(Functions::String::Lower(column->alias), columnHeader))
        continue;

      if (columnExistsOnStatement) {
        os << column->alias << " is ambigious";
        return {Errors::ValidationError::Error, os.str()};
      }

      columnExistsOnStatement = true;

      column->columnId = columnHeader.id;
      column->returnType = static_cast<DataType>(columnHeader.dataType);
      column->index = columnHeader.ordinalPosition;
    }

    if (!columnExistsOnStatement) {
      os << "Column: " << column->alias << " does not exist in the statement";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (column->name.empty())
      column->name = columnHeader.name;

    return {};
  }

  bool ValidateExpressionCoercionTypes(const Expressions::Expression *left, const Expressions::Expression *right){
    // If one side is a column expression, its type takes precedence
    const auto* leftColumn = left->AsColumn();
    const auto* rightColumn = right->AsColumn();

    if (leftColumn == nullptr && rightColumn == nullptr)
      return DataTypes::Coercions::IsCoercionAllowed(left->GetReturnType(), right->GetReturnType()) ||
           DataTypes::Coercions::IsCoercionAllowed(right->GetReturnType(), left->GetReturnType());

    if (leftColumn != nullptr && rightColumn == nullptr) {
      const auto* constantExpr = right->AsConstant();

      if (constantExpr != nullptr
        && (constantExpr->value.IsNull()
        || DataTypes::Coercions::CanBeParsedToType(leftColumn->GetReturnType(), constantExpr->value)))
        return true;

      return DataTypes::Coercions::IsCoercionAllowed(right->GetReturnType(), leftColumn->GetReturnType());
    }

    if (leftColumn == nullptr && rightColumn != nullptr) {
      const auto* constantExpr = left->AsConstant();

      if (constantExpr != nullptr
        && DataTypes::Coercions::CanBeParsedToType(rightColumn->GetReturnType(), constantExpr->value))
        return true;

      return DataTypes::Coercions::IsCoercionAllowed(left->GetReturnType(), rightColumn->GetReturnType());
    }

    if (leftColumn != nullptr && rightColumn != nullptr)
      return DataTypes::Coercions::IsCoercionAllowed(leftColumn->GetReturnType(), rightColumn->GetReturnType()) ||
             DataTypes::Coercions::IsCoercionAllowed(rightColumn->GetReturnType(), leftColumn->GetReturnType());

    return true;
  }

  bool ValidateExpressionCoercionTypes(const DataType &type, const Expressions::Expression *expression) {
    if (expression->IsConstant()) {
      auto* constantExpr = expression->AsConstant();

      if (constantExpr->value.IsNull()
        || DataTypes::Coercions::CanBeParsedToType(type, constantExpr->value))
        return true;

      return DataTypes::Coercions::IsCoercionAllowed(constantExpr->GetReturnType(), type);
    }

    return DataTypes::Coercions::IsCoercionAllowed(expression->GetReturnType(), type);
  }

  Errors::ValidationStatus CompileWildcard(
    const Expressions::ColumnExpression* column,
    const StatementValidationScope& statementValidationScope,
    SelectStatement* statement
  ){
    if (column->alias != Constants::WILDCARD)
      return {};

    if (statementValidationScope.indexPos == nullptr)
      return {Errors::ValidationError::Error, "Unexpected error occurred during wildcard validation"};

    //if no alias is specified get all the columns from the existing tables in the query
    if (column->tableAlias.empty()) {
      statement->results.erase(statement->results.begin() + *statementValidationScope.indexPos);

      for (const auto &columnsDict : statement->tableColumnsDictionary | views::values) {
        AssignColumnsFromWildCardExpression(columnsDict, column->tableAlias, statementValidationScope, statement->results);
        *statementValidationScope.indexPos += static_cast<int>(columnsDict.size());
      }

      delete column;
      return {};
    }

    //else get only from the specified
    table_id_t tableId = 0;
    if (!column->tableAlias.empty()
      && !statementValidationScope.tableAliasesDictionary->TryGetValue(column->tableAlias, tableId)) {
        ostringstream os;
        os << "Alias " << column->tableAlias << " does on exist on statement";
        return {Errors::ValidationError::Error, os.str()};
      }

    //remove the wildcard
    statement->results.erase(statement->results.begin() + *statementValidationScope.indexPos);
    AssignColumnsFromWildCardExpression(statement->tableColumnsDictionary.Get(tableId), column->tableAlias, statementValidationScope, statement->results);

    delete column;
    return {};
  }

  void AssignColumnsFromWildCardExpression(
    const Dictionary<std::string, Headers::ColumnHeader> &columnsDict,
    const std::string& tableAlias,
    const StatementValidationScope& statementValidationScope,
    std::vector<Expressions::Expression*>& results
  ) {
        results.insert(results.begin() + *statementValidationScope.indexPos, columnsDict.size(), nullptr);
        // results.resize(results.size() + columnsDict.size());
        for (const auto &header: columnsDict | views::values) {

          auto* columnExpression = new Expressions::ColumnExpression(
            header.name,
            tableAlias
          );

          columnExpression->name = header.name;
          columnExpression->columnId = header.id;
          columnExpression->tableId = header.tableId;
          columnExpression->index = header.ordinalPosition;

          const auto insertPos = *statementValidationScope.indexPos + header.ordinalPosition;

          results[insertPos] = columnExpression;
        }
  }

  void FoldExpression(Expressions::Expression *&expression) {
    switch (expression->expressionType) {
      case Expressions::ExpressionType::Binary:
        FoldExpression(expression->AsBinary(), expression);
        break;
      case Expressions::ExpressionType::Logical:
        FoldExpression(expression->AsLogical(), expression);
        break;
      case Expressions::ExpressionType::Branch:
        FoldExpression(expression->AsBranch(), expression);
        break;
      case Expressions::ExpressionType::Function:
        FoldExpression(expression->AsFunction(), expression);
        break;
      case Expressions::ExpressionType::Expression:
      case Expressions::ExpressionType::Column:
      case Expressions::ExpressionType::Constant:
      case Expressions::ExpressionType::Variable:
        break;
    }
  }

  void FoldExpression(const Expressions::BinaryExpression *castExpr, Expressions::Expression *&expression){
    if (!castExpr->left->IsConstant()
      || !castExpr->right->IsConstant())
      return;

    EvaluateExpression(expression);
  }

  void FoldExpression(Expressions::LogicalExpression *castExpr, Expressions::Expression *&expression){
    //If expression is of type OR and either right or left is a constant, it will always be true
    if (castExpr->IsOr()) {
      if (castExpr->left->IsConstant()) {
        PropagateExpression(expression, castExpr->left);
        return;
      }

      if (castExpr->right->IsConstant()) {
        PropagateExpression(expression, castExpr->right);
      }

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

  void FoldExpression(const Expressions::FunctionExpression *castExpr, Expressions::Expression *&expression){
    for (const auto& argument : castExpr->arguments) {
      if (!argument->IsConstant())
        return;
    }

    EvaluateExpression(expression);
  }

  void FoldExpression(Expressions::BranchExpression *castExpr, Expressions::Expression *&expression){
    for (int i = 0;i < castExpr->branches.size(); i++) {
      const auto* branch = castExpr->branches[i];

      if (!branch->IsConstant())
        return;

      const auto value = branch->AsConstant()->Evaluate({});
      if (value.GetBool()) {
        PropagateExpression(expression, castExpr->results[i]);
        FoldExpression(expression);
        return;
      }
    }
  }

  void PropagateExpression(Expressions::Expression *&expression, Expressions::Expression *&childExpr) {
    auto* expr = childExpr;
    childExpr = nullptr;

    delete expression;
    expression = expr;
  }

  void EvaluateExpression(Expressions::Expression *&expression) {
    auto value = expression->Evaluate({});
    delete expression;

    expression = new Expressions::ConstantExpression(value);
  }

  void AssignConstantToExpression(Expressions::Expression *&expression) {
    delete expression;
    expression = new Expressions::ConstantExpression(Value(true, 0));
  }

  void AssignColumnIndicesToExpression(
    const Dictionary<int32_t, column_index_t>& columnIndicesDictionary,
    Expressions::Expression *expression
  ){
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
    case Expressions::ExpressionType::Variable:
    case Expressions::ExpressionType::Expression:
    case Expressions::ExpressionType::Constant:
    default:
      break;
    }
  }

  void AssignColumnIndicesToBinaryExpression(
    const Dictionary<int32_t, column_index_t> &columnIndicesDictionary,
    const Expressions::BinaryExpression *expression
  ) {
    AssignColumnIndicesToExpression(columnIndicesDictionary, expression->left);
    AssignColumnIndicesToExpression(columnIndicesDictionary, expression->right);
  }

  void AssignColumnIndicesToLogicalExpression(
    const Dictionary<int32_t, column_index_t> &columnIndicesDictionary,
    const Expressions::LogicalExpression *expression
  ){
      AssignColumnIndicesToExpression(columnIndicesDictionary, expression->left);
      AssignColumnIndicesToExpression(columnIndicesDictionary, expression->right);
  }

  void AssignColumnIndicesToBranchExpression(
    const Dictionary<int32_t, column_index_t> &columnIndicesDictionary,
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
    const Dictionary<int32_t, column_index_t> &columnIndicesDictionary,
    const Expressions::FunctionExpression *expression
  ) {
    for (auto* childExpr : expression->arguments)
      AssignColumnIndicesToExpression(columnIndicesDictionary, childExpr);
  }

  void AssignColumnIndicesToColumnExpression(
    const Dictionary<int32_t, column_index_t> &columnIndicesDictionary,
    Expressions::ColumnExpression *expression
  ) {
    expression->index = columnIndicesDictionary.Get(expression->columnId);
  }

  Errors::ValidationStatus CompilePostProjectionExpression(
    Expressions::Expression *expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  ){
    switch (expression->expressionType) {
      case Expressions::ExpressionType::Binary:
        return CompilePostProjectionBinaryExpression(expression->AsBinary(), postProjectionAliases);
      case Expressions::ExpressionType::Logical:
        return CompilePostProjectionLogicalExpression(expression->AsLogical(), postProjectionAliases);
      case Expressions::ExpressionType::Branch:
        return CompilePostProjectionBranchExpression(expression->AsBranch(), postProjectionAliases);
      case Expressions::ExpressionType::Function:
        return CompilePostProjectionFunctionExpression(expression->AsFunction(), postProjectionAliases);
      case Expressions::ExpressionType::Column:
        return CompilePostProjectionColumnExpression(expression->AsColumn(), postProjectionAliases);
      case Expressions::ExpressionType::Variable:
      case Expressions::ExpressionType::Constant:
      case Expressions::ExpressionType::Expression:
      default:
        break;
    }

    return {};
  }

  Errors::ValidationStatus CompilePostProjectionColumnExpression(
    Expressions::ColumnExpression *column,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
    ){
    const Expressions::Expression* expression;
    if (!postProjectionAliases.TryGetValue(column->alias, expression)) {
      ostringstream os;
      os << "Column: " << column->alias << " does not exist in the statement";

      return {Errors::ValidationError::Error, os.str()};
    }

    column->returnType = expression->GetReturnType();
    return {};
  }

  Errors::ValidationStatus CompilePostProjectionBinaryExpression(
    const Expressions::BinaryExpression *expression,
    const Dictionary<std::string, const Expressions::Expression *> &postProjectionAliases
  ){
    return CompilePostProjectionExpression(expression->left, postProjectionAliases)
    && CompilePostProjectionExpression(expression->right, postProjectionAliases);
  }

  Errors::ValidationStatus CompilePostProjectionLogicalExpression(
    const Expressions::LogicalExpression *expression,
    const Dictionary<std::string, const Expressions::Expression *> &postProjectionAliases
  ){
    return CompilePostProjectionExpression(expression->left, postProjectionAliases)
    && CompilePostProjectionExpression(expression->right, postProjectionAliases);
  }

  Errors::ValidationStatus CompilePostProjectionFunctionExpression(
    const Expressions::FunctionExpression *expression,
    const Dictionary<std::string, const Expressions::Expression *> &postProjectionAliases
  ){
    for (auto* childExpr : expression->arguments) {
      auto childExprStatus = CompilePostProjectionExpression(childExpr, postProjectionAliases);

      if (!childExprStatus.IsOk())
        return childExprStatus;
    }

    //validate number of arguments
    std::string errorMessage;
    if (!expression->ValidateNumberOfArguments(errorMessage))
      return {Errors::ValidationError::Error, errorMessage};

    return {};
  }

  Errors::ValidationStatus CompilePostProjectionBranchExpression(
    const Expressions::BranchExpression *expression,
    const Dictionary<std::string, const Expressions::Expression *> &postProjectionAliases
  ){
    for (const auto& argument : expression->arguments) {
      auto result = CompilePostProjectionExpression(argument, postProjectionAliases);

      if (!result.IsOk())
        return result;
    }

    for (const auto& branch : expression->branches) {
      auto result = CompilePostProjectionExpression(branch, postProjectionAliases);

      if (!result.IsOk())
        return result;
    }

    for (const auto& resultExpr : expression->results) {
      auto result = CompilePostProjectionExpression(resultExpr, postProjectionAliases);

      if (!result.IsOk())
        return result;
    }

    return {};
  }

  void AssignPostProjectionIndicesToExpression(
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
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
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
    const Expressions::BinaryExpression *expression
  ){
    AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->left);
    AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->right);
  }

  void AssignPostProjectionIndicesToLogicalExpression(
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
    const Expressions::LogicalExpression *expression
  ){
    AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->left);
    AssignPostProjectionIndicesToExpression(columnIndicesDictionary, expression->right);
  }

  void AssignPostProjectionIndicesToFunctionExpression(
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
    const Expressions::FunctionExpression *expression
  ){
    for (auto* childExpr : expression->arguments)
      AssignPostProjectionIndicesToExpression(columnIndicesDictionary, childExpr);
  }

  void AssignPostProjectionIndicesToBranchExpression(
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
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
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
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
    const Dictionary<std::string, column_index_t> &columnIndicesDictionary,
    Expressions::ColumnExpression *expression
  ) {
    expression->index = columnIndicesDictionary.Get(expression->alias);
  }

  Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(const DataType &type) {
    ostringstream os;

    os  << "Expression of type: " << ColumnTypesToStringDictionary.Get(type)
        << " cannot be converted to type: Bool";

    return {Errors::ValidationError::Error, os.str()};
  }

  void TryPropagateChildExpression(
    Expressions::Expression*& expression,
    Expressions::Expression*& leftExpr,
    Expressions::Expression*& rightExpr
  ){
    const auto* left = leftExpr->AsConstant();
    if (left->value.IsNull())
      return;

    if(DataTypes::Coercions::CanBeParsedToType(DataType::Bool, left->value)
      && left->value.GetBool() == false) {
      PropagateExpression(expression, leftExpr);
      return;
      }

    PropagateExpression(expression, rightExpr);
  }
}