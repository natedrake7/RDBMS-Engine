#include "CompileContext.h"
#include "../../include/Visitor.h"
#include "../../include/Statements.h"
#include "../../../Systemic/include/Converter.h"
#include "../../include/ErrorListener.h"

namespace QueryPipeline {
    antlrcpp::Any SQLVisitorImplementation::visitCreateTableStatement(SQLParser::CreateTableStatementContext *context) {
        auto* statement = this->_compileContext->Allocate<Statements::CreateTableStatement>();

        statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

        statement->columns.TrySetAllocator(this->_compileContext->GetAllocator());
        statement->primaryKey.TrySetAllocator(this->_compileContext->GetAllocator());

        for (const auto columnContext: context->addColumn()) {
            const auto column = std::any_cast<Statements::NewColumn*>(visit(columnContext));
            statement->columns.Push(column);
        }

        if (context->primaryKeyConstraint())
            statement->constraint = std::any_cast<Statements::PrimaryKeyConstraint*>(visit(context->primaryKeyConstraint()));

        return statement;
    }

    antlrcpp::Any SQLVisitorImplementation::visitPrimaryKey(SQLParser::PrimaryKeyContext *context) {
        if (context->autoIncrementKey())
            return visit(context->autoIncrementKey());

        return nullptr;
    }

    antlrcpp::Any SQLVisitorImplementation::visitAutoIncrementKey(SQLParser::AutoIncrementKeyContext *context) {
        auto* incrementStatement = new Statements::Identity();

        incrementStatement->seed = Converter<uint8_t>::Stoi(context->seed->getText());
        incrementStatement->incrementFactor = Converter<uint8_t>::Stoi(context->increment->getText());

        return incrementStatement;
    }

    antlrcpp::Any SQLVisitorImplementation::visitAddColumn(SQLParser::AddColumnContext *context) {
        const bool isPrimaryKey = (context->primaryKey()) != nullptr;

        Statements::Identity* key = (isPrimaryKey && context->primaryKey()->autoIncrementKey())
            ? std::any_cast<Statements::Identity*>(visit(context->primaryKey()))
            : nullptr;

        const bool isNullable = ((!context->NULL_() && !context->NOT() && !isPrimaryKey)
                                 || (context->NULL_() && !context->NOT()) && !isPrimaryKey);

        if (context->primaryKey() && context->defaultValue())
            throw SyntaxError("Cannot set a primary key with a default value.", CreatePositionErrorMessage(context));

        if (context->primaryKey() && context->NULL_())
            throw SyntaxError("Cannot set a primary key with default value NULL.", CreatePositionErrorMessage(context));

        auto newColumn = this->_compileContext->Allocate<Statements::NewColumn>();
        newColumn->name = std::any_cast<Statements::ColumnName>(visit(context->columnName()));
        newColumn->type = std::any_cast<Statements::ColumnType>(visit(context->dataType()));
        newColumn->identity = key;
        newColumn->defaultValue = context->defaultValue()
            ? std::any_cast<Value>(visit(context->defaultValue()))
            : Value::Null();
        newColumn->isPrimaryKey = isPrimaryKey;
        newColumn->isNullable = isNullable;

        return std::any(newColumn);
    }
}