#include "Statements.h"

#include "../Constants.h"
#include "../../AdditionalLibraries/Coercions/Coercions.h"
#include "../../AdditionalLibraries/Functions/StringFunctions.h"
#include "../../Database/Database.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

#include <iostream>
#include <ranges>

namespace QueryPipeline::Statements {

  Statement::Statement(){
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->table = nullptr;
  }

  bool DeleteStatement::Validate(){

    if (!this->table->Validate(this->databaseId))
      return false;

    if (this->where.expression == nullptr)
      return true;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);


    return true;
    // return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->table, this->where.expression);
  }

  JoinStatement::JoinStatement() {
    this->type = Constants::JoinType::Inner;
    this->table = nullptr;
    this->expression = nullptr;
  }

  JoinStatement::~JoinStatement(){
      delete this->table;
  }

  bool JoinStatement::Validate(){
    return true;
  }

  bool JoinStatement::Validate(const int32_t& databaseId){
    this->databaseId = databaseId;

    return this->table != nullptr && this->table->Validate(this->databaseId);
  }

  QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){

    return new LogicalTableScan(this->table, nullptr);
      return nullptr;
    // return new LogicalJoin(
    //     this->databaseId,
    //     // new LogicalTableScan(),
    //     // this->joinType,
    //     // this->table2,
    //     // this->on.expression
    // );
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

  bool CreateTableStatement::Validate(){
    if (!this->table->ValidateTableCreate(this->databaseId))
      return false;

    const auto& schemasDict = Server::ServerInstance::Get().SelectSchemasToDictionary(this->databaseId);

    Headers::SchemaHeader schemaHeader;
    if (!schemasDict.TryGetValue(this->table->schema, schemaHeader)) {
      std::cerr << "Schema: " << this->table->schema << "does not exist." << std::endl;
      return false;
    }

    this->table->schemaId = schemaHeader.id;

    column_index_t tablePosition = 0;
    bool primaryKeyFound = false;
    Dictionary<string, column_index_t> columnNamesToIndexes;

    for (const auto& column: this->columns) {
      uint16_t columnSize;

      if (!ColumnTypeSizes.TryGetValue(column->type.name, columnSize)) {
        std::cerr << "Column Type: " + column->type.name + " does not exist" << endl;
        return false;
      }


      if (columnSize != 0)
        column->type.size = columnSize;

      const auto& dataType = ColumnTypesDictionary.Get(column->type.name);

      if (dataType == DataType::Decimal) {
        if (!column->type.decimal.Validate()) {
          std::cerr << "Decimal type requires precision and scale to be set correctly" << std::endl;
          return false;
        }

        column->type.size = DataTypes::Decimal::Size(column->type.decimal.precision);
      }

      column->index = tablePosition++;

      columnNamesToIndexes.Add(column->name.name, column->index);

      if(column->isPrimaryKey && primaryKeyFound){
        cerr << "Cannot have multiple primary keys defined. Consider declaring a composite key" << endl;
        return false;
      }

      if (column->isPrimaryKey) {
        this->primaryKey.push_back(column->index);
        primaryKeyFound = true;

        //store the pointer if found, else let it be null
        if(column->HasIdentity()
          && !column->identity->Validate())
            return false;
      }
    }

    if (this->constraint == nullptr)
      return true;

    if (primaryKeyFound) {
      std::cerr << "Cannot have a primary key and a constraint declared" << std::endl;
      return false;
    }

    //primary key will be clear for sure here
    for (const auto& column: this->constraint->columns)
      this->primaryKey.push_back(columnNamesToIndexes[column.name]);

    return true;
  }

  LogicalPlan * CreateTableStatement::ToLogical(){
    const auto constraintName = this->constraint == nullptr ? "" : this->constraint->name;

    return new LogicalTableCreate(this->table, this->columns, this->primaryKey, constraintName);
  }

   SelectStatement::~SelectStatement(){
      delete this->table;
      delete this->orderBy;

      for (const auto* join : this->joins) {
        delete join;
      }
  }

  bool SelectStatement::HasJoins()const{ return !this->joins.empty(); }

  bool SelectStatement::Validate(){
    if (this->table != nullptr
      && !this->table->Validate(this->databaseId))
      return false;

    if (!this->joins.empty() && this->table == nullptr) {
      std::cerr << "Table was not specified" << std::endl;
      return false;
    }

    //resolve expressions here since no column is to be used
    if (this->table == nullptr)
      return this->ValidateNoTableStatement();

    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;

    for (const auto& join: this->joins) {
      if (!join->Validate(this->databaseId))
        return false;
    }

    if (!this->ResolveAliases(aliasesDictionary))
      return false;

    return true;
  }

  bool SelectStatement::ValidateNoTableStatement(){
    for (const auto& resultExpr : this->results){
      if (!ResolveExpressionAliases(this, resultExpr))
        return false;
    }

    if (this->HasJoins()) {
      std::cerr << "Missing FROM statement but joins were given" << std::endl;
      return false;
    }

    return true;
  }

  bool SelectStatement::ResolveAliases(Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId));

    //Add all the join tables to the dictionaries
    for (const auto& join: this->joins) {
      tableAliasesDictionary.Add(join->table->GetAlias(), join->table->tableId);
      this->tableColumnsDictionary.Add(join->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(join->table->tableId));
    }

    //start resolving aliases
    for (int i = 0; i < this->results.size(); i++)
      if (!ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, this->results[i], i))
          return false;

    //validate all expressions are valid
    if (this->where.expression != nullptr) {
      if (!this->where.IsValid()) {
        std::cerr << "Where expression must be either a logical or a binary expression" << std::endl;
        return false;
      }

      if (!ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, this->where.expression, 0))
        return false;
    }

    //validate join expressions
    for (const auto& join: this->joins)
      if (!ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, join->expression, 0))
        return false;

    if (this->orderBy == nullptr)
      return true;

    Dictionary<std::string, const Expressions::Expression*> postProjectionAliases;

    for (const auto* resultExpr : this->results) {
      if (resultExpr->name.empty())
        continue;

      if (postProjectionAliases.Contains(resultExpr->name)) {
        std::cerr << resultExpr->name << " already exists on result set"<< std::endl;
        return false;
      }

      postProjectionAliases.Add(resultExpr->name, resultExpr);
    }

    for (const auto& column: this->orderBy->columns) {
      if (!ResolvePostProjectionAliases(postProjectionAliases, column->expression))
        return false;
    }

    return true;
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

  bool Identity::Validate() const{
    if (this->incrementFactor <= 0) {
      std::cerr << "Increment Factor must be greater than zero" << std::endl;
      return false;
    }



    return true;
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

   TableName::TableName() {
    this->databaseId = Constants::INVALID_DATABASE_ID;
    this->tableId = Constants::INVALID_TABLE_ID;
    this->schemaId = Constants::INVALID_SCHEMA_ID;
    this->ordinalPosition = Constants::INVALID_ORDINAL_POS;
    this->schema = "dbo";
  }

  std::string TableName::GetAlias() const{
    return this->alias.empty()
        ? this->GetFullName()
          : this->alias;
  }

  std::string TableName::GetFullName() const {
    return (this->database.empty() ? "" : this->database + ".") + this->schema + "." + this->name;
  }

  bool TableName::Validate(const int32_t& selectedDatabaseId) {
    const auto tableHeader = (!this->database.empty())
        ? Server::ServerInstance::Get().SelectTable(this->database, this->name)
        : Server::ServerInstance::Get().SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id == Constants::INVALID_TABLE_ID){
      std::cerr << "Table " + this->GetFullName() + " does not exist" << std::endl;
      return false;
    }

    this->tableId = tableHeader.id;
    this->ordinalPosition = tableHeader.ordinalPosition;
    this->databaseId = tableHeader.databaseId;

    return true;
  }

  bool TableName::ValidateTableCreate(const int32_t &selectedDatabaseId){
    const auto tableHeader = (!this->database.empty())
      ? Server::ServerInstance::Get().SelectTable(this->database, this->name)
      : Server::ServerInstance::Get().SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id != Constants::INVALID_TABLE_ID){
      std::cerr << "Table " + this->GetFullName() + " does not exist" << std::endl;
      return false;
    }

    if (this->databaseId == Constants::INVALID_DATABASE_ID)
      this->databaseId = selectedDatabaseId;

    return true;
  }

  LogicalPlan * SelectStatement::ToLogical(){
    LogicalPlan* current = (this->table != nullptr)
            ? new LogicalTableScan(this->table, this->joins.empty() ? this->where.expression : nullptr)
            : nullptr;

    //join re orders take place here
    if (this->table != nullptr) {
      Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary;
      Constants::column_index_t columnIndex = 0;

      for (const auto &columnsDict : this->tableColumnsDictionary | views::values) {
        for (const auto &column: columnsDict | views::values) {
            if (columnIndicesDictionary.Contains(column.id))
              continue;

            columnIndicesDictionary.Add(column.id, columnIndex + column.ordinalPosition);
        }

        columnIndex += columnsDict.size();
      }

      AssignColumnsToIndices(this, columnIndicesDictionary);
    }


    //here create logical joins with the expressions
    //re order here
    for (const auto& join : this->joins) {
      current = new LogicalJoin(current, join->ToLogical(), join->expression, JoinType::Inner);
    }

    Dictionary<std::string, Constants::column_index_t> postProjectionIndicesDictionary;

    for (int i = 0;i < this->results.size(); i++) {
      const auto& resultExpr = this->results[i];

      if (resultExpr->name.empty())
        continue;

      postProjectionIndicesDictionary.Add(resultExpr->name, i);
    }

    if (!this->results.empty())
      current = new LogicalProject(current, this->results, this->columnHeaders);

    if(this->orderBy != nullptr) {
      for (const auto& column : this->orderBy->columns) {
        AssignPostProjectionIndicesToExpression(postProjectionIndicesDictionary, column->expression);
      }

      current = new LogicalOrder(current, this->orderBy->columns);
    }

    return current;
  }

  bool CreateDbStatement::Validate(){
    if (!Server::ServerInstance::Get().DatabaseExists(this->name))
      return true;

    cerr << "Database " + this->name + " already exists" << endl;
    return false;
  }

  LogicalPlan * CreateDbStatement::ToLogical(){
    return new LogicalCreateDatabase(this->name);
  }

  bool DropDbStatement::Validate(){
    const auto database = Server::ServerInstance::Get().SelectDatabase(this->name);

    if (database.name.empty()) {
      cerr << "Cannot drop: " << this->name << ". Database" << this->name << " does not exist" << endl;
      return false;
    }

    if (database.isSystem) {
      cerr << "Cannot drop: " << this->name << ". Database" << this->name << " is a system database" << endl;
     return false;
    }

    return true;
  }
  
  LogicalPlan * DropDbStatement::ToLogical(){
    return nullptr;
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

  bool InsertStatement::ValidateReturnType(const Expressions::Expression* expression, const std::string& columnName) const{
    const auto valueType = expression->GetReturnType();

    const auto& columnsDictionary = this->tableColumnsDictionary.Get(this->table->tableId);

    const auto& columnHeader = columnsDictionary.Get(columnName);

    const auto columnType = static_cast<Constants::DataType>(columnHeader.dataType);

    if (DataTypes::Coercions::IsCoercionAllowed(
      valueType,
        columnType
      ))
      return true;

    const auto* literalExpr = dynamic_cast<const Expressions::LiteralExpression*>(expression);

    if (literalExpr != nullptr) {
      if (literalExpr->value.GetIsNull()) {
        if (columnHeader.isNullable)
          return true;

        std::cerr << "Column " << columnHeader.name << " does not allow NULL. Insert fails." << std::endl;
        return false;
      }

      if (DataTypes::Coercions::CanBeParsedToType(columnType, literalExpr->value))
        return true;
    }

    std::cerr << "Cannot update column " << columnHeader.name << " of type "
              << ColumnTypesToStringDictionary.Get(columnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType) << std::endl;

    return false;
  }

  bool InsertStatement::HasSelectStatement() const { return this->selectStatement != nullptr; }

  bool InsertStatement::ValidateSelectStatement()const{

    if (this->selectStatement == nullptr)
      return true;

    if (this->selectStatement->results.size() != this->columns.size()) {
      std::cerr << "Invalid number of arguments specified on select statement" << std::endl;
      return false;
    }

    this->selectStatement->databaseId = this->databaseId;

    if (!this->selectStatement->Validate())
      return false;

    for (int i = 0;i < this->selectStatement->results.size();i++) {
      const auto& resultExpression = this->selectStatement->results[i];

      if (!this->ValidateReturnType(resultExpression, this->columns[i].name))
        return false;
    }

    return true;
  }

  bool InsertStatement::ResolveAliases(){
    Dictionary<std::string, table_id_t> tableAliasesDictionary{
      {this->table->GetAlias(), this->table->tableId}
    };

    for (auto& [insertColumns] : this->values) {

      for (int i = 0;i < insertColumns.size(); i++) {
        const auto& value = insertColumns[i];

        if (!ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, value))
          return false;

        if (!this->ValidateReturnType(value, this->columns[i].name))
          return false;
      }
    }

    return true;
  }
  //TODO validate length of columns to match max record_size from master DB
  bool InsertStatement::Validate(){
    if (this->table == nullptr
      || !this->table->Validate(this->databaseId))
      return false;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    this->tableColumnsDictionary.Add(this->table->tableId, columnsDict);

    const auto identityColumns = Server::ServerInstance::Get().SelectIdentityColumnsByTableIdToDictionary(this->table->tableId);

    //validate insert columns existance
    HashSet<int32_t> statementColumns;

    for (auto & column : this->columns) {
      Headers::ColumnHeader header;

      //check if columns exist on the table
      if (!columnsDict.TryGetValue(column.name, header)) {
        std::cerr << "Column " << column.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
        return false;
      }

      //check if the specified column is an identity column
      if (identityColumns.Contains(header.id)) {
        std::cerr << "Cannot specify an identity column for insert" << std::endl;
        return false;
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
        std::cerr << "Column " << columnName << " does not allow NULLS. Insert fails" << std::endl;
        return false;
      }

      this->InsertDefaultValuesForMissingColumns(header, defaultValue);
    }

    if (this->HasSelectStatement())
      return this->ValidateSelectStatement();

    if (!this->ResolveAliases())
      return false;

    return true;
  }

  LogicalPlan* InsertStatement::ToLogical() {

    auto* logicalSelect = this->HasSelectStatement()
        ? this->selectStatement->ToLogical()
        : nullptr;

    return new QueryPipeline::LogicalInsert(this->table, this->values, logicalSelect, this->columnIndices);
  }

  bool CreateSchemaStatement::Validate(){
    if (Server::ServerInstance::Get().SchemaExists(this->databaseId, this->name)) {
      cerr << "Schema " << this->name << " already exists"  << endl;
      return false;
    }

    return true;
  }

  QueryPipeline::LogicalPlan * CreateSchemaStatement::ToLogical(){
    return new LogicalSchemaCreate(this->databaseId, this->name);
  }

  UpdateColumn::UpdateColumn(){
    this->value = nullptr;
  }

  UpdateColumn::~UpdateColumn(){
    delete this->value;
  }

  bool UpdateStatement::ValidateReturnType(const UpdateColumn* update) const{
    const auto valueType = update->value->GetReturnType();
    if (DataTypes::Coercions::IsCoercionAllowed(
      valueType,
        update->name.returnType)
        )
      return true;

    const auto* literalExpr = dynamic_cast<Expressions::LiteralExpression*>(update->value);

    if (literalExpr != nullptr) {
      if (literalExpr->value.GetIsNull()) {
        const auto& columns = this->tableColumnsDictionary.Get(this->table->tableId);

        if (columns.Get(update->name.name).isNullable)
          return true;

        std::cerr << "Column " << update->name.name << " does not allow NULL. Update fails." << std::endl;
        return false;
      }

      if (DataTypes::Coercions::CanBeParsedToType(update->name.returnType, literalExpr->value))
        return true;
    }

    std::cerr << "Cannot update column " << update->name.name << " of type "
              << ColumnTypesToStringDictionary.Get(update->name.returnType)
              << " with value of type "
              << ColumnTypesToStringDictionary.Get(valueType) << std::endl;

    return false;
  }

  bool UpdateStatement::ResolveAliases(Dictionary<std::string, table_id_t> &tableAliasesDictionary){
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(this->table->GetAlias(), this->table->tableId);
    this->tableColumnsDictionary.Add(this->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId));

    //start resolving aliases
    for (int i = 0; i < this->updates.size(); i++) {
      auto* update = this->updates.at(i);

      if (!ResolveColumnAlias(update->name, tableAliasesDictionary, this->tableColumnsDictionary))
        return false;

      if (!ResolveExpressionAliases(tableAliasesDictionary, this->tableColumnsDictionary, this, update->value, i))
          return false;

      if (!this->ValidateReturnType(update))
        return false;
    }

    //validate all expressions are valid
    if (this->where.expression != nullptr) {
      if (!this->where.IsValid()) {
        std::cerr << "Where expression must be either a logical or a binary expression" << std::endl;
        return false;
      }

      if (!ResolveExpressionAliases(tableAliasesDictionary,this->tableColumnsDictionary, this, this->where.expression))
        return false;
    }

    return true;
  }


bool UpdateStatement::Validate(){
    if (this->table == nullptr
      || !this->table->Validate(this->databaseId))
      return false;

    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;
    return this->ResolveAliases(aliasesDictionary);
  }

  QueryPipeline::LogicalPlan* UpdateStatement::ToLogical(){
    return new QueryPipeline::LogicalUpdate(this->table, this->updates, this->where.expression);
  }

  bool CreateIndexStatement::Validate(){
    if (!this->table->Validate(this->databaseId))
      return false;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    for(auto& column: this->columns) {
      Headers::ColumnHeader header;

      if (columnsDict.TryGetValue(column, header)) {
        this->columnIndices.push_back(header.ordinalPosition);

        continue;
      }

      cerr << "Column " << column << " does not exist on table: " << this->table->schema << "." << this->table->name << endl;
      return false;
    }

    const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

    for (const auto& index: indexes) {
      const auto indexedColumns = Server::ServerInstance::Get().SelectIndexColumnsByIndexIdToDictionary(index.id);

      if (index.name == this->name) {
        cerr << "Index with name: " << index.name << " already exists" << endl;
        return false;
      }

      //check if identical index exists (no need for a duplicate).
    }

    return true;
  }

  QueryPipeline::LogicalPlan * CreateIndexStatement::ToLogical(){
    return new QueryPipeline::LogicalIndexCreate(this->table, this->name, this->columnIndices);
  }

  bool AlterTableStatement::ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    if (headers.Contains(this->newColumn->name.name)) {
      std::cerr << "Column " << this->newColumn->name.name << " already exists on table: "<< this->table->GetFullName() << std::endl;
      return false;
    }

    if (!this->newColumn->isNullable
      && this->newColumn->defaultValue.GetIsNull()) {
      std::cerr << "Cannot insert default Value NULL when NOT NULL is specified" << std::endl;
      return false;
    }

    this->newColumn->index = headers.size();

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(AdditionalLibraries::StringFunctions::NormalizeString(this->newColumn->type.name), columnType)) {
      std::cerr << "Invalid Column Type " << this->newColumn->type.name << std::endl;
      return false;
    }

    const auto recordSize = ColumnTypeSizes.Get(this->newColumn->type.name);

    if (recordSize != 0)
      this->newColumn->type.size = recordSize;

    if (columnType == DataType::Decimal) {
      if (!this->newColumn->type.decimal.Validate()) {
        std::cerr << "Decimal type requires precision and scale to be set correctly" << std::endl;
        return false;
      }

      this->newColumn->type.size = DataTypes::Decimal::Size(this->newColumn->type.decimal.precision);
    }

    //TODO Check this
    // this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

    return true;
  }

  bool AlterTableStatement::ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;

    if (!headers.TryGetValue(this->alterColumn->name.name, header)) {
      std::cerr << "Column " << this->alterColumn->name.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(AdditionalLibraries::StringFunctions::NormalizeString(this->alterColumn->type.name), columnType)) {
      std::cerr << "Invalid Column Type " << this->alterColumn->type.name << std::endl;
      return false;
    }

    if ((PipelineConstants::ValidStringConversions.Contains(columnType)
      && !PipelineConstants::ValidStringConversions.Contains(static_cast<Constants::DataType>(header.dataType)))
      || (PipelineConstants::ValidIntegerConversions.Contains(columnType)
        && !PipelineConstants::ValidIntegerConversions.Contains(static_cast<Constants::DataType>(header.dataType)))){
          std::cerr << "Cannot alter column " << this->alterColumn->name.name << " from type: "
                    << ColumnTypesToStringDictionary.Get(static_cast<Constants::DataType>(header.dataType))
                    << "to type: " << this->alterColumn->type.name << std::endl;

        return false;
    }

    if (header.recordSize > this->alterColumn->type.size) {
      std::cerr << "Cannot alter column " << this->alterColumn->type.name
                << " with size " <<  header.recordSize << " to size: " << this->alterColumn->type.size
                << std::endl
                << "Use FORCE if potential data corruption is acceptable" << std::endl;
      return false;
    }

    this->alterColumn->columnId = header.id;
    return true;
  }

  bool AlterTableStatement::ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    if (!headers.TryGetValue(this->dropColumn->name.name, header)) {
      std::cerr << "Column " << this->dropColumn->name.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    //validate no index or constraint uses it
    const auto constraints = Server::ServerInstance::Get().SelectConstraints(this->table->tableId);

    for (const auto& constraint: constraints) {
      const auto columns = Server::ServerInstance::Get().SelectConstraintColumnsByConstraintIdToDictionary(constraint.constraintId);

      if (columns.Contains(header.id)) {
        std::cerr << "Cannot drop column: " << header.name << " as it is referenced by constraint: " << constraint.name << std::endl;
        return false;
      }
    }

    this->dropColumn->index = header.ordinalPosition;

    return true;
  }

  bool AlterTableStatement::ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    if (!headers.TryGetValue(this->renameColumn->oldName.name, header)) {
      std::cerr << "Column " << this->renameColumn->oldName.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    this->renameColumn->columnId = header.id;
    this->renameColumn->ordinalPosition = header.ordinalPosition;

    return true;
  }

  bool AlterTableStatement::Validate(){
    if (this->table == nullptr
     || !this->table->Validate(this->databaseId))
      return false;

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
        std::cerr << "Unknown table type" << std::endl;
        return false;
    }
  }

  QueryPipeline::LogicalPlan * AlterTableStatement::ToLogical(){
    return new LogicalAlterTable(this->table, this->type, this->alterColumn, this->newColumn, this->dropColumn, this->renameColumn);
  }

  bool ResolveColumnAlias(
    ColumnName &column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary){

    if (!column.alias.empty()) {
      table_id_t tableId;

      if (!tableAliasesDictionary.TryGetValue(column.alias, tableId)) {
        cerr << "Alias: " << column.alias << " does not exist in the statement" << endl;
        return false;
      }

      column.tableId = tableId;
    }

    bool columnExistsOnTable = false;
    Headers::ColumnHeader columnHeader;
    for (const auto& [key, columns]: tablesColumnsDictionary) {
      if (!columns.TryGetValue(column.name, columnHeader))
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
      std::cerr << "Column: " << column.alias << " does not exist on Table" << std::endl;
      return false;
    }

    return true;
  }

  bool ResolveColumnAlias(
      Expressions::ColumnExpression* column,
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
      Statement *statement,
      const int& indexPos){

        //if wildcard ensure statement is of select statement type
        if (column->alias == Constants::WILDCARD) {
          auto* selectStatement = dynamic_cast<SelectStatement*>(statement);

          return selectStatement == nullptr
              ? false
              : ResolveWildCardAlias(
                  column,
                  tableAliasesDictionary,
                  selectStatement,
                  indexPos
              );
        }

        if (!column->tableAlias.empty()) {
          table_id_t tableId;

          if (!tableAliasesDictionary.TryGetValue(column->tableAlias, tableId)) {
            cerr << "Alias: " << column->tableAlias << " does not exist in the statement" << endl;
            return false;
          }

          column->tableId = tableId;
        }

        // bool columnExistsOnTable = false;
        Headers::ColumnHeader columnHeader;
        for (const auto& [key, columns]: tablesColumnsDictionary) {
          if (!columns.TryGetValue(column->alias, columnHeader))
            continue;

          column->tableId = key;
          column->columnId = columnHeader.id;
          column->returnType = static_cast<Constants::DataType>(columnHeader.dataType);
          column->index = columnHeader.ordinalPosition;

          break;
        }

      // if (!columnExistsOnTable) {
      //   std::cerr << "Column: " << column->alias << " does not exist on Table" << std::endl;
      //   return false;
      // }

      if (column->name.empty())
        column->name = columnHeader.name;

      return true;
  }

  bool ResolvePostProjectionColumnAlias(
    Expressions::ColumnExpression *column,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
    ){

    const Expressions::Expression* expression;
    if (!postProjectionAliases.TryGetValue(column->alias, expression)) {
      std::cerr << "Column: " << column->alias << " does not exist in the statement" << std::endl;
      return false;
    }

    column->returnType = expression->GetReturnType();
    return true;
  }

  bool ResolveExpressionAliases(
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    Statement *statement,
    Expressions::Expression *expr,
    const int& indexPos){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr)) {
      return  ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->left, indexPos)
          && ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->right, indexPos)
          && ValidateExpressionCoercionTypes(binaryExpr->left, binaryExpr->right);
    }

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
      if (statement->table == nullptr) {
        std::cerr << "No table was specified but column with name: " << columnExpr->alias << " was specified." << std::endl;
        return false;
      }

      return ResolveColumnAlias(columnExpr, tableAliasesDictionary, tablesColumnsDictionary, statement, indexPos);
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {

      //validate children expressions and assign return types and ids to column expressions
      for (auto* childExpr : functionExpr->arguments) {
        if (!ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, childExpr, indexPos))
          return false;
      }

      //validate number of arguments
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        std::cerr << errorMessage << std::endl;
        return false;
      }
    }

    if (auto* literalExpr = dynamic_cast<Expressions::LiteralExpression*>(expr)) {
      DataTypes::Coercions::DeduceIntegerType(literalExpr->value);
      return true;
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->left, indexPos)
        && ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->right, indexPos)
        && ValidateExpressionCoercionTypes(logicalExpr->left, logicalExpr->right);
    }

    return true;
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

  bool ResolvePostProjectionAliases(
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
        if (!ResolvePostProjectionAliases(postProjectionAliases, childExpr))
          return false;
      }

      //validate number of arguments
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        std::cerr << errorMessage << std::endl;
        return false;
      }
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolvePostProjectionAliases(postProjectionAliases, logicalExpr->left)
        && ResolvePostProjectionAliases(postProjectionAliases, logicalExpr->right);
    }

    return true;
  }

  bool ResolveExpressionAliases(Statement *statement, Expressions::Expression *expr){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr))
      return  ResolveExpressionAliases(statement, binaryExpr->left) &&
              ResolveExpressionAliases(statement, binaryExpr->right);

    if (const auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
        std::cerr << "No table was specified but column with name: " << columnExpr->alias << " was specified." << std::endl;
        return false;
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      //validate functionExpression
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        std::cerr << errorMessage << std::endl;
        return false;
      }

      for (auto* childExpr : functionExpr->arguments) {
        if (!ResolveExpressionAliases(statement, childExpr))
          return false;
      }
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolveExpressionAliases(statement, logicalExpr->left)
        && ResolveExpressionAliases(statement, logicalExpr->right);
    }

    //TODO Validate Literals and functions

    return true;
  }

  bool ResolveWildCardAlias(
    const Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    SelectStatement *statement,
    const int& indexPos){

    if (column->alias != "*")
      return true;

    table_id_t tableId = 0;
    if (!column->tableAlias.empty()
      && !tableAliasesDictionary.TryGetValue(column->tableAlias, tableId)) {
      std::cerr << "Alias " << column->tableAlias << " does on exist on statement" << std::endl;
      return false;
    }

    if (column->tableAlias.empty())
      tableId = statement->table->tableId;

    //remove the wildcard
    statement->results.erase(statement->results.begin() + indexPos);

    int counter = 0; //insert after IndexPos, the position of the
    for (const auto &header: statement->tableColumnsDictionary.Get(tableId) | views::values) {

      auto* columnExpression = new Expressions::ColumnExpression(
            header.name,
            statement->table->GetAlias());

      columnExpression->name = header.name;
      columnExpression->columnId = header.id;
      columnExpression->tableId = statement->table->tableId;

      statement->results.insert(statement->results.begin() + indexPos + counter, columnExpression);
      counter++;
    }

    delete column;

    return true;
  }

  void AssignColumnsToIndices(SelectStatement *statement, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary){
    for (const auto& resultExpr : statement->results)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, resultExpr);

    if (statement->where.expression != nullptr)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, statement->where.expression);

    for (const auto* join : statement->joins) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, join->expression);
    }

    //group by here later
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