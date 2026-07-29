#include "../../include/Parsing/SqlParser.h"

#include "../../../CoreEngine/include/Evaluators/Expression.h"
#include "../../../CoreEngine/include/Evaluators/Expressions.Additional.h"
#include "../../../Systemic/include/Converter.h"
#include "../../../Systemic/include/DataTypes/JsonBinary.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace QueryPipeline::Parsing{

    //Longest sign-prefixed decimal literal we will construct on the stack.
    static constexpr Int MAX_DECIMAL_LITERAL_LENGTH = 64;

    SqlParser::SqlParser(
        const Token* tokens,
        const Int count,
        const ::Memory::IAllocator* allocator,
        Diagnostic& diagnostic
    )   :   _tokens(tokens), _count(count), _position(0),
            _allocator(allocator), _diagnostic(&diagnostic){}

    SqlParser::SqlParser(
        const DataStructures::PolymorphicArray<Token>& tokens,
        const ::Memory::IAllocator* allocator,
        Diagnostic& diagnostic
    )   :   SqlParser(tokens.Data(), tokens.Size(), allocator, diagnostic){}

    /**
     * @name Cursor
     * The token buffer is always EndOfFile terminated, so lookahead clamps to that last
     * token instead of bounds checking at every call site.
     * @{
     */

    const Token& SqlParser::Peek(const Int offset) const{
        const Int index = this->_position + offset;

        if (index < 0)
            return this->_tokens[0];

        return (index < this->_count)
                   ? this->_tokens[index]
                   : this->_tokens[this->_count - 1];
    }

    const Token& SqlParser::Current() const{
        return this->Peek(0);
    }

    bool SqlParser::AtEnd() const{
        return this->Current().type == TokenType::EndOfFile;
    }

    bool SqlParser::HasError() const{
        return this->_diagnostic->hasError;
    }

    bool SqlParser::Check(const TokenType type) const{
        return this->Current().type == type;
    }

    const Token& SqlParser::Advance(){
        const auto& token = this->Current();

        //Pin on the terminator rather than running off the end.
        if (this->_position < this->_count - 1)
            ++this->_position;

        return token;
    }

    bool SqlParser::Match(const TokenType type){
        if (!this->Check(type))
            return false;

        this->Advance();
        return true;
    }

    bool SqlParser::Expect(const TokenType type){
        if (this->Match(type))
            return true;

        const auto& token = this->Current();
        this->_diagnostic->Raise(
            "unexpected token", token.line, token.column,
            token.View(), TokenTypeName(type)
        );

        return false;
    }

    std::nullptr_t SqlParser::Fail(const DataTypes::StringView& message) const{
        const auto& token = this->Current();
        this->_diagnostic->Raise(message, token.line, token.column, token.View());
        return nullptr;
    }

    /** @} */

    DataTypes::String SqlParser::MakeString(const Token& token) const{
        return DataTypes::String(token.View(), this->_allocator);
    }

    /**
     * @name Precedence cascade
     * One function per precedence level, loosest first, mirroring the expression rule
     * in SQL.g4. All binary levels are left associative.
     * @{
     */

    Expressions::Expression* SqlParser::ParseExpression(){
        return this->ParseOr();
    }

    Expressions::Expression* SqlParser::ParseOr(){
        auto* left = this->ParseAnd();

        while (left != nullptr && this->Match(TokenType::Or)){
            auto* right = this->ParseAnd();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::LogicalExpression>(
                left, right, Expressions::LogicalType::Or
            );
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseAnd(){
        auto* left = this->ParseNot();

        while (left != nullptr && this->Match(TokenType::And)){
            auto* right = this->ParseNot();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::LogicalExpression>(
                left, right, Expressions::LogicalType::And
            );
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseNot(){
        if (!this->Match(TokenType::Not))
            return this->ParseEquality();

        auto* operand = this->ParseEquality();
        if (operand == nullptr)
            return nullptr;

        //NOT is unary: the operand goes on the left and the right stays null.
        return this->_allocator->Allocate<Expressions::LogicalExpression>(
            operand, nullptr, Expressions::LogicalType::Not
        );
    }

    Expressions::Expression* SqlParser::ParseEquality(){
        auto* left = this->ParseRelational();

        while (left != nullptr){
            Expressions::BinaryOperator operation;

            if (this->Check(TokenType::Equal))
                operation = Expressions::BinaryOperator::Equal;
            else if (this->Check(TokenType::NotEqual))
                operation = Expressions::BinaryOperator::NotEqual;
            else
                break;

            this->Advance();

            auto* right = this->ParseRelational();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::BinaryExpression>(left, right, operation);
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseRelational(){
        auto* left = this->ParseAdditive();

        while (left != nullptr){
            Expressions::BinaryOperator operation;

            switch (this->Current().type){
                case TokenType::Less:         operation = Expressions::BinaryOperator::Less; break;
                case TokenType::LessEqual:    operation = Expressions::BinaryOperator::LessEqual; break;
                case TokenType::Greater:      operation = Expressions::BinaryOperator::Greater; break;
                case TokenType::GreaterEqual: operation = Expressions::BinaryOperator::GreaterEqual; break;
                default:                      return left;
            }

            this->Advance();

            auto* right = this->ParseAdditive();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::BinaryExpression>(left, right, operation);
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseAdditive(){
        auto* left = this->ParseMultiplicative();

        while (left != nullptr){
            Expressions::BinaryOperator operation;

            if (this->Check(TokenType::Plus))
                operation = Expressions::BinaryOperator::Add;
            else if (this->Check(TokenType::Minus))
                operation = Expressions::BinaryOperator::Subtract;
            else
                break;

            this->Advance();

            auto* right = this->ParseMultiplicative();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::BinaryExpression>(left, right, operation);
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseMultiplicative(){
        auto* left = this->ParseUnary();

        while (left != nullptr){
            Expressions::BinaryOperator operation;

            switch (this->Current().type){
                case TokenType::Star:    operation = Expressions::BinaryOperator::Multiply; break;
                case TokenType::Slash:   operation = Expressions::BinaryOperator::Divide; break;
                case TokenType::Percent: operation = Expressions::BinaryOperator::Modulo; break;
                default:                 return left;
            }

            this->Advance();

            auto* right = this->ParseUnary();
            if (right == nullptr)
                return nullptr;

            left = this->_allocator->Allocate<Expressions::BinaryExpression>(left, right, operation);
        }

        return left;
    }

    Expressions::Expression* SqlParser::ParseUnary(){
        //Unary plus is a no-op.
        if (this->Match(TokenType::Plus))
            return this->ParseUnary();

        if (!this->Match(TokenType::Minus))
            return this->ParsePostfix();

        //Fold the sign into the literal where we can.
        if (this->Check(TokenType::IntegerLiteral) || this->Check(TokenType::DecimalLiteral))
            return this->ParseNumericLiteral(true);

        auto* operand = this->ParseUnary();
        if (operand == nullptr)
            return nullptr;

        //The engine has no unary expression node, so negation becomes (0 - operand).
        auto zero = Value(static_cast<BigInt>(0), this->_allocator, 0);
        auto* left = this->_allocator->Allocate<Expressions::ConstantExpression>(std::move(zero));

        return this->_allocator->Allocate<Expressions::BinaryExpression>(
            left, operand, Expressions::BinaryOperator::Subtract
        );
    }

    Expressions::Expression* SqlParser::ParsePostfix(){
        auto* expression = this->ParsePrimary();
        if (expression == nullptr)
            return nullptr;

        if (!this->Check(TokenType::JsonObjectAccessor) && !this->Check(TokenType::JsonScalarAccessor))
            return expression;

        //JsonExpression is anchored on a column, so nothing else can carry a path.
        if (!expression->IsColumn())
            return this->Fail("the -> and ->> accessors can only be applied to a column");

        return this->ApplyJsonAccessors(expression->AsColumn());
    }

    /** @} */

    Expressions::Expression* SqlParser::ParsePrimary(){
        switch (this->Current().type){
            case TokenType::LeftParen: {
                this->Advance();

                auto* inner = this->ParseExpression();
                if (inner == nullptr)
                    return nullptr;

                return this->Expect(TokenType::RightParen) ? inner : nullptr;
            }

            case TokenType::StringLiteral:
            case TokenType::UnicodeStringLiteral:
            case TokenType::True:
            case TokenType::False:
            case TokenType::Null:
                return this->ParseLiteral();

            case TokenType::IntegerLiteral:
            case TokenType::DecimalLiteral:
                return this->ParseNumericLiteral(false);

            case TokenType::Variable:
                return this->ParseVariable();

            case TokenType::Cast:
            case TokenType::TryCast:
                return this->ParseCast();

            case TokenType::Switch:
                return this->ParseSwitch();

            case TokenType::Iif:
                return this->ParseIif();

            default:
                break;
        }

        //A name followed by '(' is a call. This is what lets LEFT and RIGHT be both
        //join keywords and function names without any grammar gymnastics.
        if (this->IsFunctionCallAhead())
            return this->ParseFunctionCall(this->Current());

        if (CanBeIdentifier(this->Current().type) || this->Check(TokenType::Star))
            return this->ParseColumnReference();

        return this->Fail("expected an expression");
    }

    bool SqlParser::IsFunctionCallAhead() const{
        return CanBeIdentifier(this->Current().type)
            && this->Peek(1).type == TokenType::LeftParen;
    }

    Expressions::Expression* SqlParser::ParseLiteral(){
        const auto& token = this->Advance();

        switch (token.type){
            case TokenType::StringLiteral:
            case TokenType::UnicodeStringLiteral: {
                //The text already lives in the arena, so Value can borrow it directly.
                auto text = this->MakeString(token);
                return this->_allocator->Allocate<Expressions::ConstantExpression>(
                    Value(text, this->_allocator, 0)
                );
            }

            case TokenType::True:
                return this->_allocator->Allocate<Expressions::ConstantExpression>(
                    Value(true, this->_allocator, 0)
                );

            case TokenType::False:
                return this->_allocator->Allocate<Expressions::ConstantExpression>(
                    Value(false, this->_allocator, 0)
                );

            case TokenType::Null:
                return this->_allocator->Allocate<Expressions::ConstantExpression>(
                    Value::Null(this->_allocator)
                );

            default:
                return this->Fail("expected a literal");
        }
    }

    bool SqlParser::MakeNumericValue(const Token& token, const bool negated, Value& value) const{
        if (token.type == TokenType::IntegerLiteral){
            if (!Converter::TryStrToInt<BigInt>(token.View())){
                this->Fail("integer literal is out of range");
                return false;
            }

            const auto number = Converter::StrToInt<BigInt>(token.View());
            value = Value(negated ? -number : number, this->_allocator, 0);

            return true;
        }

        if (token.length + 1 > MAX_DECIMAL_LITERAL_LENGTH){
            this->Fail("decimal literal is too long");
            return false;
        }

        //Decimal has no unary minus, so the sign is prepended before it is parsed.
        char buffer[MAX_DECIMAL_LITERAL_LENGTH];
        Int length = 0;

        if (negated)
            buffer[length++] = '-';

        std::memcpy(buffer + length, token.text, token.length);
        length += token.length;

        auto decimal = DataTypes::Decimal(DataTypes::StringView(buffer, length));
        value = Value(decimal, this->_allocator, 0);

        return true;
    }

    Expressions::Expression* SqlParser::ParseNumericLiteral(const bool negated){
        const auto& token = this->Advance();

        Value value;
        if (!this->MakeNumericValue(token, negated, value))
            return nullptr;

        return this->_allocator->Allocate<Expressions::ConstantExpression>(std::move(value));
    }

    Expressions::Expression* SqlParser::ParseVariable(){
        auto name = this->MakeString(this->Advance());
        return this->_allocator->Allocate<Expressions::VariableExpression>(name, this->_allocator);
    }

    Expressions::Expression* SqlParser::ParseColumnReference(){
        auto tableAlias = DataTypes::String::Null();

        if (this->Check(TokenType::Star))
            return this->_allocator->Allocate<Expressions::ColumnExpression>(
                this->MakeString(this->Advance()), tableAlias
            );

        const Token& first = this->Advance();

        if (!this->Match(TokenType::Dot))
            return this->_allocator->Allocate<Expressions::ColumnExpression>(
                this->MakeString(first), tableAlias
            );

        tableAlias = this->MakeString(first);

        if (this->Check(TokenType::Star))
            return this->_allocator->Allocate<Expressions::ColumnExpression>(
                this->MakeString(this->Advance()), tableAlias
            );

        if (!CanBeIdentifier(this->Current().type))
            return this->Fail("expected a column name after '.'");

        return this->_allocator->Allocate<Expressions::ColumnExpression>(
            this->MakeString(this->Advance()), tableAlias
        );
    }

    Expressions::Expression* SqlParser::ParseFunctionCall(const Token& nameToken){
        Constants::FunctionType functionType;

        //FUNCTION_TYPE_DICT hashes case insensitively, so the raw token view is enough.
        if (!Expressions::FUNCTION_TYPE_DICT.TryGetValue(nameToken.View(), functionType))
            return this->Fail("unknown function");

        this->Advance(); //the name

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        DataStructures::PolymorphicArray<Expressions::Expression*> arguments(this->_allocator);

        if (!this->Check(TokenType::RightParen)){
            do {
                auto* argument = this->ParseExpression();
                if (argument == nullptr)
                    return nullptr;

                arguments.Push(argument);
            }
            while (this->Match(TokenType::Comma));
        }

        if (!this->Expect(TokenType::RightParen))
            return nullptr;

        return this->_allocator->Allocate<Expressions::FunctionExpression>(functionType, arguments);
    }

    Expressions::Expression* SqlParser::ParseCast(){
        const bool isTryCast = this->Advance().type == TokenType::TryCast;

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        auto* child = this->ParseExpression();
        if (child == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::As))
            return nullptr;

        ParsedDataType target;
        if (!this->ParseDataType(target))
            return nullptr;

        if (!this->Expect(TokenType::RightParen))
            return nullptr;

        return this->_allocator->Allocate<Expressions::CastExpression>(child, target.type, isTryCast);
    }

    Expressions::Expression* SqlParser::ParseSwitch(){
        this->Advance(); //SWITCH

        auto* expression = this->_allocator->Allocate<Expressions::BranchExpression>(
            Expressions::BranchType::Switch, this->_allocator
        );

        while (this->Match(TokenType::Case)){
            auto* condition = this->ParseExpression();
            if (condition == nullptr)
                return nullptr;

            if (!this->Expect(TokenType::Then))
                return nullptr;

            auto* result = this->ParseExpression();
            if (result == nullptr)
                return nullptr;

            expression->branches.Push(condition);
            expression->results.Push(result);
        }

        if (!this->Expect(TokenType::Default))
            return nullptr;

        expression->baseCase = this->ParseExpression();

        return (expression->baseCase == nullptr) ? nullptr : expression;
    }

    Expressions::Expression* SqlParser::ParseIif(){
        this->Advance(); //IIF

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        auto* condition = this->ParseExpression();
        if (condition == nullptr || !this->Expect(TokenType::Comma))
            return nullptr;

        auto* trueResult = this->ParseExpression();
        if (trueResult == nullptr || !this->Expect(TokenType::Comma))
            return nullptr;

        auto* falseResult = this->ParseExpression();
        if (falseResult == nullptr || !this->Expect(TokenType::RightParen))
            return nullptr;

        auto* expression = this->_allocator->Allocate<Expressions::BranchExpression>(
            Expressions::BranchType::Ternary, this->_allocator
        );

        expression->branches.Push(condition);
        expression->results.Push(trueResult);
        expression->results.Push(falseResult);

        return expression;
    }

    Expressions::Expression* SqlParser::ApplyJsonAccessors(Expressions::ColumnExpression* column){
        auto* expression = this->_allocator->Allocate<Expressions::JsonExpression>(column, this->_allocator);

        while (this->Check(TokenType::JsonObjectAccessor) || this->Check(TokenType::JsonScalarAccessor)){
            const auto accessorType = (this->Advance().type == TokenType::JsonScalarAccessor)
                                          ? DataTypes::JsonAccessorType::Scalar
                                          : DataTypes::JsonAccessorType::Json;

            DataTypes::String key;
            if (!this->ParseJsonKey(key))
                return nullptr;

            expression->pathSegments.Push(DataTypes::JsonPathStep(std::move(key), accessorType));
        }

        //A path ending in ->> unwraps to a scalar; anything else stays Json.
        if (expression->pathSegments.Back()->_accessorType == DataTypes::JsonAccessorType::Scalar)
            expression->type = DataType::String;

        return expression;
    }

    bool SqlParser::ParseJsonKey(DataTypes::String& key){
        const auto type = this->Current().type;

        if (!CanBeIdentifier(type)
            && type != TokenType::StringLiteral
            && type != TokenType::IntegerLiteral
        ){
            this->Fail("expected a json key: a name, a string, or an integer");
            return false;
        }

        key = this->MakeString(this->Advance());
        return true;
    }

    bool SqlParser::ParseDataType(ParsedDataType& result){
        switch (this->Current().type){
            case TokenType::Bool:
                result.type = DataType::Bool;
                break;
            case TokenType::TinyInt:
                result.type = DataType::TinyInt;
                break;
            case TokenType::SmallInt:
                result.type = DataType::SmallInt;
                break;
            case TokenType::Int:
                result.type = DataType::Int;
                break;
            case TokenType::BigInt:
                result.type = DataType::BigInt;
                break;
            case TokenType::DateTime:
                result.type = DataType::DateTime;
                break;
            case TokenType::Guid:
                result.type = DataType::Guid;
                break;
            case TokenType::Json:
                result.type = DataType::Json;
                break;
            case TokenType::String: {
                this->Advance();
                result.type = DataType::String;

                if (!this->Expect(TokenType::LeftParen))
                    return false;

                if (this->Match(TokenType::Max))
                    result.size = MAX_STRING_SIZE;
                else {
                    if (!this->Check(TokenType::IntegerLiteral)){
                        this->Fail("expected a string width or MAX");
                        return false;
                    }

                    const auto& width = this->Advance();
                    if (!Converter::TryStrToInt<Int>(width.View())){
                        this->Fail("string width is out of range");
                        return false;
                    }

                    result.size = Converter::StrToInt<Int>(width.View());
                }

                return this->Expect(TokenType::RightParen);
            }

            case TokenType::Decimal: {
                this->Advance();
                result.type = DataType::Decimal;

                if (!this->Expect(TokenType::LeftParen))
                    return false;

                if (!this->Check(TokenType::IntegerLiteral)){
                    this->Fail("expected a decimal precision");
                    return false;
                }

                const auto& precision = this->Advance();
                result.precision = static_cast<TinyInt>(Converter::StrToInt<Int>(precision.View()));

                if (!this->Expect(TokenType::Comma))
                    return false;

                if (!this->Check(TokenType::IntegerLiteral)){
                    this->Fail("expected a decimal scale");
                    return false;
                }

                const auto& scale = this->Advance();
                result.scale = static_cast<TinyInt>(Converter::StrToInt<Int>(scale.View()));

                return this->Expect(TokenType::RightParen);
            }

            default:
                this->Fail("expected a data type");
                return false;
        }

        this->Advance();
        return true;
    }
}
