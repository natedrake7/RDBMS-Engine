#include "CompileContext.h"
#include "ErrorListener.h"
#include "../../../Systemic/include/Converter.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../Systemic/include/DataTypes/JsonBinary.h"
#include "../../include/Visitor.h"
#include "../../include/Statements.h"


namespace QueryPipeline{
    antlrcpp::Any SQLVisitorImplementation::visitJsonExpression(SQLParser::JsonExpressionContext* context){
        auto columnName = std::any_cast<DataTypes::String>(visit(context->identifier()));
        auto tableAlias = DataTypes::String::Null();

        if (context->columnAlias())
            tableAlias = std::any_cast<DataTypes::String>(visit(context->columnAlias()));

        auto* columnPtr = this->_compileContext->Allocate<Expressions::ColumnExpression>(columnName, tableAlias);

        auto* jsonExpression = this->_compileContext->Allocate<Expressions::JsonExpression>(columnPtr, this->_compileContext->GetAllocator());

        for (const auto& accessor : context->jsonAccessor()){
            auto pathSegment = std::any_cast<DataTypes::JsonPathStep>(visit(accessor));
            jsonExpression->pathSegments.Push(std::move(pathSegment));
        }

        return std::any(jsonExpression);
    }

    antlrcpp::Any SQLVisitorImplementation::visitJsonAccessor(SQLParser::JsonAccessorContext* context){
        if (context->jsonObjectAccessor())
            return visit(context->jsonObjectAccessor());
        if (context->jsonScalarAccessor())
            return visit(context->jsonScalarAccessor());

        throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));
    }

    antlrcpp::Any SQLVisitorImplementation::visitJsonObjectAccessor(SQLParser::JsonObjectAccessorContext* context){
        if (!context->jsonKey())
            throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));

        auto step = DataTypes::JsonPathStep(
            std::any_cast<DataTypes::JsonKey>(visit(context->jsonKey())),
            DataTypes::JsonAccessorType::Json
        );

        return std::any(step);
    }

    antlrcpp::Any SQLVisitorImplementation::visitJsonScalarAccessor(SQLParser::JsonScalarAccessorContext* context){
        if (!context->jsonKey())
            throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));

        auto step = DataTypes::JsonPathStep(
           std::any_cast<DataTypes::JsonKey>(visit(context->jsonKey())),
            DataTypes::JsonAccessorType::Scalar
        );

        return std::any(step);
    }

    antlrcpp::Any SQLVisitorImplementation::visitJsonKey(SQLParser::JsonKeyContext* context){
        DataTypes::JsonKey key;
        if (context->STRING()) {
            const auto str = Functions::String::RemoveQuotesFromString(context->STRING()->getText());
            key._type = DataTypes::JsonKeyType::Key;
            key._data._key = DataTypes::String(str, this->_compileContext->GetAllocator());
            return std::any(key);
        }

        if (context->identifier()) {
             const auto str = context->identifier()->getText();
            key._type = DataTypes::JsonKeyType::Key;
            key._data._key = DataTypes::String(str, this->_compileContext->GetAllocator());
            return std::any(key);
        }

        if (context->INT()){
            const auto str = context->INT()->getText();
            key._type = DataTypes::JsonKeyType::Index;
            key._data._arrayIndex = Converter<Int>::Stoi(str);
            return std::any(key);
        }

        throw SyntaxError("Failed to parse result value: " + context->getText(), CreatePositionErrorMessage(context));
    }
}
