#pragma once
#include "Token.h"
#include "Diagnostic.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace QueryPipeline::Parsing{

    /**
     * Hand written SQL scanner. Tokens borrow their text from the query buffer, so
     * scanning allocates nothing at all.
     *
     * Matching is longest-first ("->>" beats "->", "<=" beats "<") and keywords are
     * recognised case insensitively, matching the caseInsensitive option in SQL.g4.
     *
     * The lexer never throws. On bad input it emits TokenType::Invalid and records the
     * reason in the supplied diagnostic.
     */
    class Lexer final{
        const char* _current;
        const char* _end;
        const char* _lineStart;

        UnsignedInt _line;

        //Where the lexeme being scanned started. Captured before it is consumed so a
        //literal spanning newlines still reports the position it opened at.
        UnsignedInt _tokenLine;
        UnsignedInt _tokenColumn;

        Diagnostic* _diagnostic;

        [[nodiscard]] bool AtEnd() const;
        [[nodiscard]] char Peek(Int offset = 0) const;

        void SkipWhitespace();
        void ConsumeNewLine();

        [[nodiscard]] Token Emit(TokenType type, const char* textBegin, Int textLength) const;

        [[nodiscard]] Token ScanIdentifier(const char* tokenStart);
        [[nodiscard]] Token ScanQuotedIdentifier();
        [[nodiscard]] Token ScanVariable(const char* tokenStart);
        [[nodiscard]] Token ScanNumber(const char* tokenStart);
        [[nodiscard]] Token ScanString(TokenType type);
        [[nodiscard]] Token ScanOperator(const char* tokenStart);

        [[nodiscard]] Token Fail(
            const DataTypes::StringView& message,
            const char* textBegin,
            Int textLength
        );

        public:
            Lexer(const char* data, Int size, Diagnostic& diagnostic);
            Lexer(const DataTypes::StringView& source, Diagnostic& diagnostic);

            /**
             * Scans the next lexeme. Returns a TokenType::EndOfFile token, repeatedly,
             * once the input is exhausted.
             */
            [[nodiscard]] Token Next();

            [[nodiscard]] static constexpr bool IsDigit(const char c){
                return c >= '0' && c <= '9';
            }

            [[nodiscard]] static constexpr bool IsIdentifierStart(const char c){
                return (c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || c == '_';
            }

            [[nodiscard]] static constexpr bool IsIdentifierPart(const char c){
                return Lexer::IsIdentifierStart(c) || Lexer::IsDigit(c);
            }
    };

    /**
     * Scans an entire query into @p tokens, always terminated by an EndOfFile token so
     * the parser can look ahead without bounds checks.
     *
     * @p tokens must already have an allocator; tokens are appended, not replaced.
     * @return false if a lexical error was recorded in @p diagnostic.
     */
    bool Tokenize(
        const DataTypes::StringView& source,
        DataStructures::PolymorphicArray<Token>& tokens,
        Diagnostic& diagnostic
    );
}
