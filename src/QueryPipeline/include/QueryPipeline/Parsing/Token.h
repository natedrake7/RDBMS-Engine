#pragma once
#include <Systemic/DataTypes/StringView.h>

namespace QueryPipeline::Parsing{

    struct TokenText{
        const char* _text;
    };

    struct UsableAsIdentifier{};
    struct EndsClause{};

    inline constexpr UsableAsIdentifier AsName{};
    inline constexpr EndsClause Clause{};

    consteval TokenText Spelled(const std::string_view text){
        return TokenText{
            std::define_static_string(text)
        };
    }

    /**
     * All token kinds produced by the hand written SQL lexer.
     *
     * The keyword block must stay contiguous between FirstKeyword and LastKeyword,
     * so keyword membership is a single range check instead of a table lookup.
     */
    enum class TokenType : UnsignedSmallInt {
        EndOfFile               [[=Spelled("end of input")]] = 0,
        Invalid                 [[=Spelled("invalid token")]],

        //Literals and names
        Identifier              [[=Spelled("identifier"), = AsName]],
        Variable                [[=Spelled("variable")]],
        IntegerLiteral          [[=Spelled("integer literal")]],
        DecimalLiteral          [[=Spelled("decimal literal")]],
        StringLiteral           [[=Spelled("string literal")]],
        UnicodeStringLiteral    [[=Spelled("unicode string literal")]],

        //Punctuation
        LeftParen               [[=Spelled("(")]],
        RightParen              [[=Spelled(")")]],
        Comma                   [[=Spelled(",")]],
        Semicolon               [[=Spelled(";")]],
        Dot                     [[=Spelled(".")]],

        //Arithmetic operators
        Plus                    [[=Spelled("+")]],
        Minus                   [[=Spelled("-")]],
        Star                    [[=Spelled("*")]],
        Slash                   [[=Spelled("/")]],
        Percent                 [[=Spelled("%")]],

        //Comparison operators
        Equal                   [[=Spelled("=")]],
        NotEqual                [[=Spelled("!=")]],
        Less                    [[=Spelled("<")]],
        LessEqual               [[=Spelled("<=")]],
        Greater                 [[=Spelled(">")]],
        GreaterEqual            [[=Spelled(">=")]],

        //Json accessors
        JsonObjectAccessor      [[=Spelled("->")]],
        JsonScalarAccessor      [[=Spelled("->>")]],

        //Keywords. Keep contiguous, see FirstKeyword / LastKeyword below.
        Select,
        From                    [[=Clause]],
        Where                   [[=Clause]],
        Order                   [[=AsName, =Clause]],
        By                      [[=AsName, =Clause]],
        Values                  [[=AsName, =Clause]],
        Into                    [[=AsName, =Clause]],
        Database                [[=AsName]],
        Use                     [[=AsName]],
        Create                  [[=AsName]],
        Drop                    [[=AsName]],
        Insert                  [[=AsName]],
        Delete                  [[=AsName]],
        Update                  [[=AsName]],
        On                      [[=AsName, =Clause]],
        Table                   [[=AsName]],
        Default,
        To                      [[=AsName, =Clause]],
        Top                     [[=AsName]],
        Distinct                [[=AsName]],
        With                    [[=AsName]],
        Password                [[=AsName]],
        Role                    [[=AsName]],
        Grant                   [[=AsName]],
        User                    [[=AsName]],
        Schema                  [[=AsName]],

        Declare                 [[=AsName]],
        Set                     [[=AsName, =Clause]],

        Alter                   [[=AsName]],
        Rename                  [[=AsName]],
        Add                     [[=AsName]],
        Column                  [[=AsName]],

        Unique                  [[=AsName]],
        Index                   [[=AsName]],
        Primary                 [[=AsName]],
        Key                     [[=AsName]],
        Identity                [[=AsName]],
        Constraint              [[=AsName]],

        Count                   [[=AsName]],
        Sum                     [[=AsName]],
        Avg                     [[=AsName]],
        Min                     [[=AsName]],
        Max                     [[=AsName]],

        Not,
        Null,

        Desc                    [[=AsName]],
        Asc                     [[=AsName]],

        Left                    [[=AsName, =Clause]],
        Right                   [[=AsName, =Clause]],
        Full                    [[=AsName, =Clause]],
        Inner                   [[=AsName, =Clause]],
        Outer                   [[=AsName, =Clause]],
        Join                    [[=AsName, =Clause]],

        As                      [[=AsName]],

        And,
        Or,

        Switch,
        Case,
        When                    [[=AsName]],   //lexed for the SWITCH family but no rule consumes it
        Then,
        Iif,

        Cast,
        TryCast                 [[=Spelled("try_cast")]],

        True,
        False,

        Bool                    [[=AsName]],
        TinyInt                 [[=AsName]],
        SmallInt                [[=AsName]],
        Int                     [[=AsName]],
        BigInt                  [[=AsName]],
        Decimal                 [[=AsName]],
        DateTime                [[=AsName]],
        Guid                    [[=AsName]],
        Json                    [[=AsName]],
        String                  [[=AsName]],

        FirstKeyword = Select,
        LastKeyword = String,
    };

    inline constexpr std::size_t TOKEN_TYPE_COUNT = static_cast<std::size_t>(TokenType::LastKeyword) + 1;

    [[nodiscard]] constexpr bool IsKeyword(const TokenType type){
        return type >= TokenType::FirstKeyword && type <= TokenType::LastKeyword;
    }

    namespace Detail{
        consteval bool IsAlias(const std::meta::info token){
            return token == ^^TokenType::FirstKeyword || token == ^^TokenType::LastKeyword;
        }

        consteval std::string_view SourceText(const std::meta::info token){
            const auto text = std::meta::annotations_of_with_type(token, ^^TokenText);
            return (text.empty())
                ? std::meta::identifier_of(token)
                : std::string_view(std::meta::extract<TokenText>(text[0])._text);
        }

        consteval std::string_view UpperCased(const std::string_view text){
            std::string upper(text);
            for (auto& c : upper)
                if (c >= 'a' && c <= 'z')
                    c -= 'a' - 'A';
            return std::define_static_string(upper);
        }

        template<typename Trait>
        inline constexpr auto TOKENS_WITH = []{
            std::array<bool, TOKEN_TYPE_COUNT> table{};
            template for (constexpr auto token: Reflection::Enumerators<TokenType>){
                if constexpr (!IsAlias(token) && Reflection::HasAnnotation<Trait>(token))
                    table[static_cast<std::size_t>([:token:])] = true;
            }

            return table;
        }();

        template<bool UpperKeywords>
        inline constexpr auto TOKEN_TEXTS = []{
            std::array<DataTypes::StringView, TOKEN_TYPE_COUNT> texts{};
            template for (constexpr auto token : Reflection::Enumerators<TokenType>){
                if constexpr (!IsAlias(token)){
                    constexpr auto text = UpperKeywords && IsKeyword([:token:])
                        ? UpperCased(SourceText(token))
                        : SourceText(token);
                    texts[static_cast<std::size_t>([:token:])] =
                        DataTypes::StringView(text.data(), static_cast<Int>(text.size()));
                }
            }
            return texts;
        }();
    }

    [[nodiscard]] constexpr DataTypes::StringView TokenSourceText(const TokenType type){
        return DataTypes::StringView::ViewOf(Detail::TOKEN_TEXTS<false>[static_cast<std::size_t>(type)]);
    }

    [[nodiscard]] constexpr bool CanBeIdentifier(const TokenType type){
        const auto index = static_cast<std::size_t>(type);
        return index < TOKEN_TYPE_COUNT && Detail::TOKENS_WITH<UsableAsIdentifier>[index];
    }

    [[nodiscard]] constexpr bool IsClauseKeyword(const TokenType type){
        const auto index = static_cast<std::size_t>(type);
        return index < TOKEN_TYPE_COUNT && Detail::TOKENS_WITH<EndsClause>[index];
    }

    [[nodiscard]] constexpr DataTypes::StringView TokenTypeName(const TokenType type){
        const auto index = static_cast<std::size_t>(type);
        return index < TOKEN_TYPE_COUNT
            ? Detail::TOKEN_TEXTS<true>[index]
            : DataTypes::StringView("unknown token");
    }

    /**
     * A single lexeme.
     *
     * @c Text points straight into the query buffer handed to the lexer, so a token
     * costs nothing to produce and stays valid only for as long as that buffer does.
     * Anything that outlives the parse must be copied into the compilation arena.
     *
     * @c Text carries the semantic content rather than the raw source span: quotes are
     * stripped from string literals and brackets from quoted identifiers. Escape
     * sequences inside string literals are left untouched for the parser to resolve.
     */
    struct Token {
        const char* _text;
        Int _length;

        UnsignedInt line;   //1 based
        UnsignedInt column; //1 based

        TokenType type;

        [[nodiscard]] DataTypes::StringView View() const{
            return DataTypes::StringView::ViewOf(this->_text, this->_length);
        }
    };
}
