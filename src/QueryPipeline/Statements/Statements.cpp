#include "Statements.h"

#include "../Constants.h"
#include "../../Systemic/Coercions/Coercions.h"
#include "../../Systemic/Functions/StringFunctions.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"
#include "../../Server/Server.Constants.h"

#include <iostream>
#include <ranges>

namespace QueryPipeline::Statements {

  Statement::Statement(){
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->table = nullptr;
  }

  Errors::ValidationStatus Statement::ValidateBase()const{
    const auto* session = Server::ServerInstance::Get().GetSession(this->sessionId);

    if (!session || !session->user || !session->user->role)
      return {Errors::ValidationError::Error, "Failed to get user session"};

    if (!session->user->role->HasPermission(this->RequiredPermissions())) {
      ostringstream os;
      os  << "User: " << session->user->name << " is not authorized to perform this action.";

      return {Errors::ValidationError::Error, os.str()};
    }

    return {Errors::ValidationError::Ok, ""};
  }

  Errors::ValidationStatus Statement::ValidateStatement(){
    auto result = this->ValidateBase();

    if (!result.IsOk())
      return result;

    return this->Validate();
  }

  Errors::ValidationStatus CreateUserStatement::Validate(){
    if (this->username.empty())
      return {Errors::ValidationError::Error,  "username cannot be empty"};

    if (this->password.empty())
      return {Errors::ValidationError::Error,  "password cannot be empty"};

    if (this->role.empty())
      return {Errors::ValidationError::Error,  "role cannot be empty"};

    const auto& server = Server::ServerInstance::Get();

    ostringstream os;
    if (server.UserExists(this->username)) {
      os << "User with username: " << this->username << " already exists.";

      return {Errors::ValidationError::Error,  os.str()};
    }

    if (!server.RoleExists(this->role)) {
      os << "Role: " << this->role << " does not exist.";

      return {Errors::ValidationError::Error,  os.str()};
    }

    return {Errors::ValidationError::Ok,  ""};
  }

  Security::Permission CreateUserStatement::RequiredPermissions() const{
      return Server::ServerConstants::ADMIN_PERMISSIONS;
  }

  LogicalPlan* CreateUserStatement::ToLogical(){
    return new LogicalCreateUser(this->sessionId, this->username, this->password, this->role);
  }

  Errors::ValidationStatus GrantRoleStatement::Validate(){
    const auto& server = Server::ServerInstance::Get();

    ostringstream os;
    if (!server.UserExists(this->username)) {
      os << "User: " << this->username << " does not exist.";
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!server.RoleExists(this->role)) {
      os << "Role: " << this->role << " does not exist.";
      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  Security::Permission GrantRoleStatement::RequiredPermissions() const{
    return Server::ServerConstants::ADMIN_PERMISSIONS;
  }

  LogicalPlan * GrantRoleStatement::ToLogical(){
    return new LogicalGrantRole(this->sessionId, this->username, this->role);
  }

  Errors::ValidationStatus DeleteStatement::Validate(){
    auto result = this->table->Validate(this->databaseId);

    if (!result.IsOk())
      return result;

    if (this->where.expression == nullptr)
      return {};

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    return {};
    // return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->table, this->where.expression);
  }

  Security::Permission DeleteStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_WRITER_PERMISSIONS;
  }

  JoinStatement::JoinStatement() {
    this->type = Constants::JoinType::Inner;
    this->table = nullptr;
    this->expression = nullptr;
  }

  JoinStatement::~JoinStatement(){
      delete this->table;
  }

  Errors::ValidationStatus JoinStatement::Validate(){
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
    return Server::ServerConstants::DB_READER_PERMISSIONS;
  }

  CreateTableStatement::~CreateTableStatement() {
      delete this->constraint;
      delete this->table;

      for(const auto& column : this->columns)
          delete column;
  }

  // QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){
  //   return new LogicalJoin(this->table, this->joinType, this->table2, this->on.expression);
  // }

  CreateTableStatement::CreateTableStatement(){
    this->table = nullptr;
    this->constraint = nullptr;
  }

  Errors::ValidationStatus CreateTableStatement::Validate(){
    auto result = this->table->ValidateTableCreate(this->databaseId);

    if (!result.IsOk())
      return result;

    ostringstream os;

    const auto& schemasDict = Server::ServerInstance::Get().SelectSchemasToDictionary(this->databaseId);
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
    return Server::ServerConstants::DB_OWNER_PERMISSIONS;
  }

  SelectStatement::SelectStatement(){
    this->top = Constants::INVALID_TOP;
    this->distinct = false;
    this->orderBy = nullptr;
  }

  SelectStatement::~SelectStatement(){
      // delete this->table;
      delete this->orderBy;

      for (const auto* join : this->joins) {
        delete join;
      }
  }

  Dictionary<std::string, Constants::column_index_t> SelectStatement::CreatePostProjectionIndicesDictionary() const{
    Dictionary<std::string, Constants::column_index_t> dict;

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

  Errors::ValidationStatus SelectStatement::Validate(){
    ostringstream os;
    if (!this->joins.empty() && this->table == nullptr) {
      os << "Joins were specified but no calling table was not specified";
      return {Errors::ValidationError::Error, os.str()};
    }

    //resolve expressions here since no column is to be used
    if (this->table == nullptr)
      return this->ValidateNoTableStatement();

    auto tableResult = this->table->Validate(this->databaseId);
    if (!tableResult.IsOk())
        return tableResult;

    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;

    for (const auto& join: this->joins) {
      auto joinResult = join->Validate(this->databaseId);
      if (!joinResult.IsOk())
        return joinResult;
    }

    return this->ResolveAliases(aliasesDictionary);
  }

  Errors::ValidationStatus SelectStatement::ValidateNoTableStatement(){
    for (const auto& resultExpr : this->results){
      auto exprResult = ResolveExpressionAliases(this, resultExpr);

      if (!exprResult.IsOk())
        return exprResult;
    }

    if (this->HasJoins())
      return {Errors::ValidationError::Error, "Missing FROM statement but joins were given"};

    return {};
  }

  Errors::ValidationStatus SelectStatement::ResolveAliases(Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId));

    //Add all the join tables to the dictionaries
    for (const auto& join: this->joins) {
      tableAliasesDictionary.Add(join->table->GetAlias(), join->table->tableId);
      this->tableColumnsDictionary.Add(join->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(join->table->tableId));
    }

    //start resolving aliases
    for (int i = 0; i < this->results.size(); i++) {
      auto expressionResult = ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, this->results[i], &i);

      if (!expressionResult.IsOk())
        return expressionResult;
    }


    int indexPos = 0;
    //validate all expressions are valid
    if (this->where.expression != nullptr) {
      if (!this->where.IsValid())
        return {Errors::ValidationError::Error, "Where expression must be either a logical or a binary expression"};

      auto expressionResult = ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, this->where.expression, &indexPos);
      if (!expressionResult.IsOk())
        return expressionResult;
    }

    //validate join expressions

    for (const auto& join: this->joins) {
      auto expressionResult = ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, join->expression);

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
      auto postProjectionExpr = ResolvePostProjectionAliases(postProjectionAliases, column->expression);

      if (!postProjectionExpr.IsOk())
        return postProjectionExpr;
    }

    return {};
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
        || dynamic_cast<Expressions::LogicalExpression*>(this->expression)
        || dynamic_cast<Expressions::BinaryExpression*>(this->expression);
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

  OrderByStatement::~OrderByStatement(){
    for (const auto* column: this->columns)
      delete column;
  }

   DataSource::DataSource() {
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->tableId = Constants::INVALID_TABLE_ID;
    this->schemaId = Constants::INVALID_SCHEMA_ID;
    this->ordinalPosition = Constants::INVALID_ORDINAL_POS;
    this->schema = "dbo";
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
        ? Server::ServerInstance::Get().SelectTable(this->database, this->name)
        : Server::ServerInstance::Get().SelectTable(selectedDatabaseId, this->name, this->schema);

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
      ? Server::ServerInstance::Get().SelectTable(this->database, this->name)
      : Server::ServerInstance::Get().SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id != Constants::INVALID_TABLE_ID){
      ostringstream os;
      os << "Table " + this->GetFullName() + " exists";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->databaseId = selectedDatabaseId;

    return {};
  }

  LogicalPlan * SelectStatement::ToLogical(){
    if (this->table == nullptr)
      return new LogicalProject(nullptr, this->results, this->columnHeaders);

    LogicalPlan* current = new LogicalTableScan(this->table, this->joins.empty() ? this->where.expression : nullptr);

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

    Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary;
    Constants::column_index_t columnIndex = 0;

    for (const auto& tableId: joinOrder) {
      //TODO cache them at the beginning
      const auto& columns = Server::ServerInstance::Get().SelectColumns(tableId);

      for (const auto &column : columns) {
        if (columnIndicesDictionary.Contains(column.id))
          continue;

        columnIndicesDictionary.Add(column.id, columnIndex + column.ordinalPosition);
      }

      columnIndex += columns.size();
    }

    AssignColumnsToIndices(this, columnIndicesDictionary);

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

  Security::Permission SelectStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_READER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateDbStatement::Validate(){
    if (Server::ServerInstance::Get().DatabaseExists(this->name)) {
      ostringstream os;
      os << "Database " + this->name + " already exists";

      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  LogicalPlan * CreateDbStatement::ToLogical(){
    return new LogicalCreateDatabase(this->sessionId, this->name);
  }

  Security::Permission CreateDbStatement::RequiredPermissions() const{
    return Server::ServerConstants::ADMIN_PERMISSIONS;
  }

   Errors::ValidationStatus DropDbStatement::Validate(){
    ostringstream os;

    const auto database = Server::ServerInstance::Get().SelectDatabase(this->name);

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
    return Server::ServerConstants::ADMIN_PERMISSIONS;
  }

  Errors::ValidationStatus UseDatabaseStatement::Validate(){
    const auto dbHeader = Server::ServerInstance::Get().SelectDatabase(this->name);

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

  Security::Permission UseDatabaseStatement::RequiredPermissions() const{
    return Server::ServerConstants::GUEST_PERMISSIONS;
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
      .index = static_cast<Constants::column_index_t>(header.ordinalPosition),
      .returnType = static_cast<Constants::DataType>(header.dataType),
    });

    //Insert the default value
    for (auto& [insertColumns] : this->values) {
      const auto* data = reinterpret_cast<const unsigned char*>(defaultValue.value.data());

      insertColumns.emplace_back(
          new Expressions::LiteralExpression(Value(
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
      .index = static_cast<Constants::column_index_t>(header.ordinalPosition),
      .returnType = static_cast<Constants::DataType>(header.dataType),
    });

    for (auto& [insertColumns] : this->values)
      insertColumns.emplace_back(new Expressions::LiteralExpression(Value(nullptr, header.ordinalPosition)));
  }

  Errors::ValidationStatus InsertStatement::ValidateReturnType(const Expressions::Expression* expression, const std::string& columnName) const{
    const auto valueType = expression->GetReturnType();

    const auto& columnsDictionary = this->tableColumnsDictionary.Get(this->table->tableId);

    const auto& columnHeader = columnsDictionary.Get(Functions::String::Lower(columnName));

    const auto columnType = static_cast<Constants::DataType>(columnHeader.dataType);

    if (DataTypes::Coercions::IsCoercionAllowed(
      valueType,
        columnType
      ))
      return {};

    const auto* literalExpr = dynamic_cast<const Expressions::LiteralExpression*>(expression);

    ostringstream os;

    if (literalExpr != nullptr) {
      if (literalExpr->value.GetIsNull()) {
        if (columnHeader.isNullable)
          return {};


        os << "Column " << columnHeader.name << " does not allow NULL. Insert fails.";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (DataTypes::Coercions::CanBeParsedToType(columnType, literalExpr->value))
        return {};
    }

    os << "Cannot update column " << columnHeader.name << " of type "
              << ColumnTypesToStringDictionary.Get(columnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType);

    return {Errors::ValidationError::Error, os.str()};
  }

  bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

  Errors::ValidationStatus InsertStatement::ValidateSelectStatement()const{

    if (this->selectStatement == nullptr)
      return {};

    if (this->selectStatement->results.size() != this->columns.size())
      return {Errors::ValidationError::Error, "Invalid number of arguments specified on select statement"};

    this->selectStatement->databaseId = this->databaseId;

    auto selectStatus = this->selectStatement->Validate();
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

  Errors::ValidationStatus InsertStatement::ResolveAliases(){
    Dictionary<std::string, table_id_t> tableAliasesDictionary{
      {this->table->GetAlias(), this->table->tableId}
    };

    for (auto& [insertColumns] : this->values) {

      for (int i = 0;i < insertColumns.size(); i++) {
        const auto& value = insertColumns[i];

        auto expressionStatus = ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, value);
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
  Errors::ValidationStatus InsertStatement::Validate(){
    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "No table was specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    this->tableColumnsDictionary.Add(this->table->tableId, columnsDict);

    const auto identityColumns = Server::ServerInstance::Get().SelectIdentityColumnsByTableIdToDictionary(this->table->tableId);

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

      this->columnIndices.emplace_back(static_cast<Constants::column_index_t>(header.ordinalPosition));

      //Insert the null value
      if (header.isNullable) {
        this->InsertNullValuesForMissingColumns(header);
        continue;
      }

      const auto defaultValue = Server::ServerInstance::Get().SelectDefaultValueByColumnId(header.id);

      if (defaultValue.columnId == Constants::INVALID_COLUMN_ID) {
        os << "Column " << columnName << " does not allow NULLS. Insert fails";
        return {Errors::ValidationError::Error, os.str()};
      }

      this->InsertDefaultValuesForMissingColumns(header, defaultValue);
    }

    return (this->HasSelectStatement())
      ? this->ValidateSelectStatement()
      : this->ResolveAliases();
  }

  LogicalPlan* InsertStatement::ToLogical() {

    auto* logicalSelect = this->HasSelectStatement()
        ? this->selectStatement->ToLogical()
        : nullptr;

    return new QueryPipeline::LogicalInsert(this->table, this->values, logicalSelect, this->columnIndices);
  }

  Security::Permission InsertStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_WRITER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateSchemaStatement::Validate(){
    if (Server::ServerInstance::Get().SchemaExists(this->databaseId, this->name)) {
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
    return Server::ServerConstants::DB_OWNER_PERMISSIONS;
  }

  UpdateColumn::UpdateColumn(){
    this->value = nullptr;
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

    const auto* literalExpr = dynamic_cast<Expressions::LiteralExpression*>(update->value);

    if (literalExpr != nullptr) {
      if (literalExpr->value.GetIsNull()) {
        const auto& columns = this->tableColumnsDictionary.Get(this->table->tableId);

        if (columns.Get(update->name.name).isNullable)
          return {};

        os << "Column " << update->name.name << " does not allow NULL. Update fails.";
        return {Errors::ValidationError::Error, os.str()};
      }

      if (DataTypes::Coercions::CanBeParsedToType(update->name.returnType, literalExpr->value))
        return {};
    }

    os << "Cannot update column " << update->name.name << " of type "
              << ColumnTypesToStringDictionary.Get(update->name.returnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType);

    return {Errors::ValidationError::Error, os.str()};
  }

  Errors::ValidationStatus UpdateStatement::ResolveAliases(Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId));

    //start resolving aliases
    for (int i = 0; i < this->updates.size(); i++) {
      auto* update = this->updates.at(i);

      auto columnAliasStatus = ResolveColumnAlias(update->name, tableAliasesDictionary, this->tableColumnsDictionary);
      if (!columnAliasStatus.IsOk())
        return columnAliasStatus;

      auto expressionStatus = ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, update->value);
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

      auto expessionStatus = ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, this->where.expression);
      if (!expessionStatus.IsOk())
        return expessionStatus;
    }

    return {};
  }


Errors::ValidationStatus UpdateStatement::Validate(){

    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "Table was not specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;
    return this->ResolveAliases(aliasesDictionary);
  }

  QueryPipeline::LogicalPlan* UpdateStatement::ToLogical(){
    return new QueryPipeline::LogicalUpdate(this->table, this->updates, this->where.expression);
  }

  Security::Permission UpdateStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_WRITER_PERMISSIONS;
  }

  Errors::ValidationStatus CreateIndexStatement::Validate(){
    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

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

    const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

    for (const auto& index: indexes) {
      const auto indexedColumns = Server::ServerInstance::Get().SelectIndexColumnsByIndexIdToDictionary(index.id);

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

  Security::Permission CreateIndexStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_OWNER_PERMISSIONS;
  }

  Errors::ValidationStatus AlterTableStatement::ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    ostringstream os;
    if (headers.Contains(Functions::String::NormalizeString(this->newColumn->name.name))) {
      os << "Column " << this->newColumn->name.name << " already exists on table: "<< this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    if (!this->newColumn->isNullable
      && this->newColumn->defaultValue.GetIsNull()) {
      os << "Cannot insert default Value NULL when NOT NULL is specified";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->newColumn->index = headers.size();

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(Functions::String::NormalizeString(this->newColumn->type.name), columnType)) {
      os << "Invalid Column Type " << this->newColumn->type.name;
      return {Errors::ValidationError::Error, os.str()};
    }

    const auto recordSize = ColumnTypeSizes.Get(this->newColumn->type.name);

    if (recordSize != 0)
      this->newColumn->type.size = recordSize;

    if (columnType == DataType::Decimal) {
      if (!this->newColumn->type.decimal.Validate()) {
        os << "Decimal type requires precision and scale to be set correctly";
        return {Errors::ValidationError::Error, os.str()};
      }

      this->newColumn->type.size = DataTypes::Decimal::Size(this->newColumn->type.decimal.precision);
    }

    //TODO Check this
    // this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

    return {};
  }

  Errors::ValidationStatus AlterTableStatement::ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;

    ostringstream os;
    if (!headers.TryGetValue(Functions::String::NormalizeString(this->alterColumn->name.name), header)) {
      os << "Column " << this->alterColumn->name.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(Functions::String::NormalizeString(this->alterColumn->type.name), columnType)) {
      os << "Invalid Column Type " << this->alterColumn->type.name;
      return {Errors::ValidationError::Error, os.str()};
    }

    if ((PipelineConstants::ValidStringConversions.Contains(columnType)
      && !PipelineConstants::ValidStringConversions.Contains(static_cast<Constants::DataType>(header.dataType)))
      || (PipelineConstants::ValidIntegerConversions.Contains(columnType)
        && !PipelineConstants::ValidIntegerConversions.Contains(static_cast<Constants::DataType>(header.dataType)))){
          os << "Cannot alter column " << this->alterColumn->name.name << " from type: "
                    << ColumnTypesToStringDictionary.Get(static_cast<Constants::DataType>(header.dataType))
                    << "to type: " << this->alterColumn->type.name;

        return {Errors::ValidationError::Error, os.str()};
    }

    if (header.recordSize > this->alterColumn->type.size) {
      os << "Cannot alter column " << this->alterColumn->type.name
                << " with size " <<  header.recordSize << " to size: " << this->alterColumn->type.size
                << std::endl
                << "Use FORCE if potential data corruption is acceptable";
      return {Errors::ValidationError::Error, os.str()};
    }

    this->alterColumn->columnId = header.id;
    return {};
  }

  Errors::ValidationStatus AlterTableStatement::ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    ostringstream os;

    Headers::ColumnHeader header;
    if (!headers.TryGetValue(Functions::String::NormalizeString(this->dropColumn->name.name), header)) {
      os << "Column " << this->dropColumn->name.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};
    }

    //validate no index or constraint uses it
    const auto constraints = Server::ServerInstance::Get().SelectConstraints(this->table->tableId);

    for (const auto& constraint: constraints) {
      const auto columns = Server::ServerInstance::Get().SelectConstraintColumnsByConstraintIdToDictionary(constraint.constraintId);

      if (columns.Contains(header.id)) {
        os << "Cannot drop column: " << header.name << " as it is referenced by constraint: " << constraint.name;
        return {Errors::ValidationError::Error, os.str()};
      }
    }

    this->dropColumn->index = header.ordinalPosition;
    return {};
  }

  Errors::ValidationStatus AlterTableStatement::ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    if (!headers.TryGetValue(Functions::String::NormalizeString(this->renameColumn->oldName.name), header)) {
      ostringstream os;
      os << "Column " << this->renameColumn->oldName.name << " does not exist on table: " << this->table->GetFullName();
      return {Errors::ValidationError::Error, os.str()};;
    }

    this->renameColumn->columnId = header.id;
    this->renameColumn->ordinalPosition = header.ordinalPosition;

    return {};
  }

  Errors::ValidationStatus AlterTableStatement::Validate(){
    if (this->table == nullptr)
      return {Errors::ValidationError::Error, "Table was not specified"};

    auto tableStatus = this->table->Validate(this->databaseId);
    if (!tableStatus.IsOk())
      return tableStatus;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    //validate by type
    switch (this->type) {
      case AlterTableType::AddColumn:
        return this->ValidateAddColumn(columnsDict);
      case AlterTableType::AlterColumn:
        return this->ValidateAlterColumn(columnsDict);
      case AlterTableType::DropColumn:
        return this->ValidateDropColumn(columnsDict);
      case AlterTableType::RenameColumn:
        return this->ValidateRenameColumn(columnsDict);
      default:
        return {Errors::ValidationError::Error, "Unknown table type"};
    }
  }

  QueryPipeline::LogicalPlan * AlterTableStatement::ToLogical(){
    return new LogicalAlterTable(
      this->sessionId,
      this->table,
      this->type,
      this->alterColumn,
      this->newColumn,
      this->dropColumn,
      this->renameColumn
    );
  }

  Security::Permission AlterTableStatement::RequiredPermissions() const{
    return Server::ServerConstants::DB_OWNER_PERMISSIONS;
  }

  Errors::ValidationStatus ResolveColumnAlias(
    ColumnName &column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary){

    ostringstream os;
    if (!column.alias.empty()) {
      table_id_t tableId;

      if (!tableAliasesDictionary.TryGetValue(column.alias, tableId)) {
        os << "Alias: " << column.alias << " does not exist in the statement";
        return {Errors::ValidationError::Error, os.str()};
      }

      column.tableId = tableId;
    }

    bool columnExistsOnTable = false;
    Headers::ColumnHeader columnHeader;
    for (const auto& [key, columns]: tablesColumnsDictionary) {
      if (!columns.TryGetValue(Functions::String::Lower(column.name), columnHeader))
        continue;

      if (!columnExistsOnTable) {
        columnExistsOnTable = true;
        column.tableId = key;
        column.columnId = columnHeader.id;
        column.index = columnHeader.ordinalPosition;
        column.returnType = static_cast<Constants::DataType>(columnHeader.dataType);
      }
    }

    if (!columnExistsOnTable) {
      os << "Column: " << column.name << " does not exist on Table";
      return {Errors::ValidationError::Error, os.str()};
    }

    return {};
  }

  Errors::ValidationStatus ResolveColumnAlias(
      Expressions::ColumnExpression* column,
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
      Statement *statement,
      int* indexPos){
        //if wildcard ensure statement is of select statement type
        if (column->alias == Constants::WILDCARD) {
          auto* selectStatement = dynamic_cast<SelectStatement*>(statement);

          if (selectStatement != nullptr)
            return ResolveWildCardAlias(column, tableAliasesDictionary,selectStatement, indexPos);

          return {Errors::ValidationError::Error, ""};
        }

       return column->HasTableAlias()
            ? ResolveColumnAliasWhenTableAliasExists(column, tableAliasesDictionary, tablesColumnsDictionary)
            : ResolveColumnAliasWhenTableAliasDoesNotExist(column, tableAliasesDictionary, tablesColumnsDictionary);
  }

  Errors::ValidationStatus ResolveColumnAliasWhenTableAliasExists(
    Expressions::ColumnExpression *column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary
  ){
    ostringstream os;

    table_id_t tableId;
    Headers::ColumnHeader columnHeader;

    if (!tableAliasesDictionary.TryGetValue(column->tableAlias, tableId)) {
      os << "Alias: " << column->tableAlias << " does not exist in the statement";
      return {Errors::ValidationError::Error, os.str()};
    }

    column->tableId = tableId;

    const auto& columns = tablesColumnsDictionary.Get(column->tableId);

    if (!columns.TryGetValue(Functions::String::Lower(column->alias), columnHeader)) {
      os << "column: " << column->alias << " does not exist in the statement";
      return {Errors::ValidationError::Error, os.str()};
    }

    column->columnId = columnHeader.id;
    column->returnType = static_cast<Constants::DataType>(columnHeader.dataType);
    column->index = columnHeader.ordinalPosition;

    if (column->name.empty())
      column->name = columnHeader.name;

    return {};
  }

  Errors::ValidationStatus ResolveColumnAliasWhenTableAliasDoesNotExist(
    Expressions::ColumnExpression *column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary
  ){
    ostringstream os;

    Headers::ColumnHeader columnHeader;
    bool columnExistsOnStatement = false;

    for (const auto &columns : tablesColumnsDictionary | views::values) {
      if (!columns.TryGetValue(Functions::String::Lower(column->alias), columnHeader))
        continue;

      if (columnExistsOnStatement) {
        os << column->alias << " is ambigious";
        return {Errors::ValidationError::Error, os.str()};
      }

      columnExistsOnStatement = true;

      column->columnId = columnHeader.id;
      column->returnType = static_cast<Constants::DataType>(columnHeader.dataType);
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

  Errors::ValidationStatus ResolvePostProjectionColumnAlias(
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

  Errors::ValidationStatus ResolveExpressionAliases(
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    Statement *statement,
    Expressions::Expression *expr,
    int* indexPos){

    ostringstream os;
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr)) {
      auto result = ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->left, indexPos)
            && ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->right, indexPos);

      if (!result.IsOk())
        return result;

      if (!ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right)) {
        const auto& leftTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->left->GetReturnType());
        const auto& rightTypeStr = ColumnTypesToStringDictionary.Get(binaryExpr->right->GetReturnType());

        os << "Invalid conversion between " << leftTypeStr << "and " << rightTypeStr <<".Use explicit cast";
        return {Errors::ValidationError::Error, os.str()};
      }
    }

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
      if (statement->table == nullptr) {
        os << "No table was specified but column with name: " << columnExpr->alias << " was specified.";
        return {Errors::ValidationError::Error, os.str()};
      }

      return ResolveColumnAlias(columnExpr, tableAliasesDictionary, tablesColumnsDictionary, statement, indexPos);
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {

      //validate children expressions and assign return types and ids to column expressions
      for (auto* childExpr : functionExpr->arguments) {
        auto childExpressionResult = ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, childExpr, indexPos);

        if (!childExpressionResult.IsOk())
          return childExpressionResult;
      }

      //validate number of arguments
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage))
        return {Errors::ValidationError::Error, errorMessage};
    }

    if (auto* literalExpr = dynamic_cast<Expressions::LiteralExpression*>(expr)) {
      DataTypes::Coercions::DeduceIntegerType(literalExpr->value);
      return {};
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      auto result = ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->left, indexPos)
                && ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->right, indexPos);

      if (!result.IsOk())
        return result;

      //validate type
      if (!ValidateExpressionCoercionTypes(logicalExpr->left, logicalExpr->right)) {

        const auto& leftTypeStr = ColumnTypesToStringDictionary.Get(logicalExpr->left->GetReturnType());
        const auto& rightTypeStr = ColumnTypesToStringDictionary.Get(logicalExpr->right->GetReturnType());

        os << "Invalid conversion between " << leftTypeStr << "and " << rightTypeStr <<".Use explicit cast";

        return {Errors::ValidationError::Error, os.str()};
      }
    }

    return {};
  }

  bool ValidateExpressionCoercionTypes(const Expressions::Expression *left, const Expressions::Expression *right){
    // If one side is a column expression, its type takes precedence
    const auto* leftColumn = dynamic_cast<const Expressions::ColumnExpression*>(left);
    const auto* rightColumn = dynamic_cast<const Expressions::ColumnExpression*>(right);

    if (leftColumn == nullptr && rightColumn == nullptr)
      return DataTypes::Coercions::IsCoercionAllowed(left->GetReturnType(), right->GetReturnType()) ||
           DataTypes::Coercions::IsCoercionAllowed(right->GetReturnType(), left->GetReturnType());

    if (leftColumn != nullptr && rightColumn == nullptr) {
      const auto* literalExpr = dynamic_cast<const Expressions::LiteralExpression*>(right);

      if (literalExpr != nullptr
        && (literalExpr->value.GetIsNull()
        || DataTypes::Coercions::CanBeParsedToType(leftColumn->GetReturnType(), literalExpr->value)))
        return true;

      return DataTypes::Coercions::IsCoercionAllowed(right->GetReturnType(), leftColumn->GetReturnType());
    }

    if (leftColumn == nullptr && rightColumn != nullptr) {
      const auto* literalExpr = dynamic_cast<const Expressions::LiteralExpression*>(left);

      if (literalExpr != nullptr
        && DataTypes::Coercions::CanBeParsedToType(rightColumn->GetReturnType(), literalExpr->value))
        return true;

      return DataTypes::Coercions::IsCoercionAllowed(left->GetReturnType(), rightColumn->GetReturnType());
    }
    if (leftColumn != nullptr && rightColumn != nullptr)
      return DataTypes::Coercions::IsCoercionAllowed(leftColumn->GetReturnType(), rightColumn->GetReturnType()) ||
             DataTypes::Coercions::IsCoercionAllowed(rightColumn->GetReturnType(), leftColumn->GetReturnType());

    return true;
  }

  Errors::ValidationStatus ResolvePostProjectionAliases(
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases,
    Expressions::Expression *expr){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr))
      return  ResolvePostProjectionAliases(postProjectionAliases, binaryExpr->left)
          && ResolvePostProjectionAliases(postProjectionAliases, binaryExpr->right);

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr))
      return ResolvePostProjectionColumnAlias(columnExpr, postProjectionAliases);

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      //validate children expressions and assign return types and ids to column expressions
      for (auto* childExpr : functionExpr->arguments) {
        auto childExprStatus = ResolvePostProjectionAliases(postProjectionAliases, childExpr);

        if (!childExprStatus.IsOk())
          return childExprStatus;
      }

      //validate number of arguments
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage))
        return {Errors::ValidationError::Error, errorMessage};
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type
      return ResolvePostProjectionAliases(postProjectionAliases, logicalExpr->left)
        && ResolvePostProjectionAliases(postProjectionAliases, logicalExpr->right);
    }

    return {};
  }

  Errors::ValidationStatus ResolveExpressionAliases(Statement *statement, Expressions::Expression *expr){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr))
      return  ResolveExpressionAliases(statement, binaryExpr->left) &&
              ResolveExpressionAliases(statement, binaryExpr->right);

    if (const auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
        ostringstream os;
        os << "No table was specified but column with name: " << columnExpr->alias << " was specified.";

        return {Errors::ValidationError::Error, os.str()};
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      //validate functionExpression
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        return {Errors::ValidationError::Error, errorMessage};
      }

      for (auto* childExpr : functionExpr->arguments) {
        auto childExpressionResult = ResolveExpressionAliases(statement, childExpr);

        if (!childExpressionResult.IsOk())
          return childExpressionResult;
      }
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolveExpressionAliases(statement, logicalExpr->left)
        && ResolveExpressionAliases(statement, logicalExpr->right);
    }

    //TODO Validate Literals and functions

    return {};
  }

  Errors::ValidationStatus ResolveWildCardAlias(
    const Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    SelectStatement *statement,
    int* indexPos){

    if (column->alias != Constants::WILDCARD)
      return {};

    if (indexPos == nullptr)
      return {Errors::ValidationError::Error, "Unexpected error occured during wildcard validation"};

    //if no alias is specified get all the columns from the existing tables in the query
    if (column->tableAlias.empty()) {
      statement->results.erase(statement->results.begin() + *indexPos);

      int counter = 0; //insert after IndexPos, the position of the
      for (const auto &[tableId, tableColumns] : statement->tableColumnsDictionary) {
        for (const auto &header: tableColumns | views::values) {

          auto* columnExpression = new Expressions::ColumnExpression(
            header.name,
            statement->table->GetAlias()
          );

          columnExpression->name = header.name;
          columnExpression->columnId = header.id;
          columnExpression->tableId = tableId;

          statement->results.insert(statement->results.begin() + *indexPos + counter, columnExpression);
          counter++;
        }
      }

      *indexPos += counter;

      delete column;
      return {};
    }

    //else get only from the specified
    table_id_t tableId = 0;
    if (!column->tableAlias.empty()
      && !tableAliasesDictionary.TryGetValue(column->tableAlias, tableId)) {
        ostringstream os;
        os << "Alias " << column->tableAlias << " does on exist on statement";
        return {Errors::ValidationError::Error, os.str()};
      }


    //remove the wildcard
    statement->results.erase(statement->results.begin() + *indexPos);

    int counter = 0; //insert after IndexPos, the position of the
    for (const auto &header: statement->tableColumnsDictionary.Get(tableId) | views::values) {

      auto* columnExpression = new Expressions::ColumnExpression(
            header.name,
            statement->table->GetAlias());

      columnExpression->name = header.name;
      columnExpression->columnId = header.id;
      columnExpression->tableId = statement->table->tableId;

      statement->results.insert(statement->results.begin() + *indexPos + counter, columnExpression);
      counter++;
    }

    delete column;

    return {};
  }

  void AssignColumnsToIndices(SelectStatement *statement, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary){
    for (const auto& resultExpr : statement->results)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, resultExpr);

    if (statement->where.expression != nullptr)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, statement->where.expression);

    for (const auto* join : statement->joins)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, join->expression);
  }

  void AssignColumnIndicesToResultExpression(
    SelectStatement *statement,
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression *expr){

    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr)) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, binaryExpr->left);
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, binaryExpr->right);
      return;
    }

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
      columnExpr->index = columnIndicesDictionary.Get(columnExpr->columnId);
      return;
    }

    if (const auto* funcExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      for (auto* childExpr : funcExpr->arguments)
        AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, childExpr);
      return;
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, logicalExpr->left);
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, logicalExpr->right);
    }
  }

  void AssignPostProjectionIndicesToExpression(
    const Dictionary<std::string, Constants::column_index_t> &columnIndicesDictionary,
    Expressions::Expression *expr){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr)) {
      AssignPostProjectionIndicesToExpression(columnIndicesDictionary, binaryExpr->left);
      AssignPostProjectionIndicesToExpression(columnIndicesDictionary, binaryExpr->right);
      return;
    }

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
      columnExpr->index = columnIndicesDictionary.Get(columnExpr->alias);
      return;
    }

    if (const auto* funcExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      for (auto* childExpr : funcExpr->arguments)
        AssignPostProjectionIndicesToExpression(columnIndicesDictionary, childExpr);
      return;
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      AssignPostProjectionIndicesToExpression(columnIndicesDictionary, logicalExpr->left);
      AssignPostProjectionIndicesToExpression(columnIndicesDictionary, logicalExpr->right);
    }
  }

};