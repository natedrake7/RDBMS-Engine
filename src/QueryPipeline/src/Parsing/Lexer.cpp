#include "../../include/Parsing/Lexer.h"
#include "../../include/Parsing/Keywords.h"

namespace QueryPipeline::Parsing{

    Lexer::Lexer(const char* data, const Int size, Diagnostic& diagnostic)
        :   _current(data), _end(data + size), _lineStart(data),
            _line(1), _tokenLine(1), _tokenColumn(1),
            _diagnostic(&diagnostic){}

    Lexer::Lexer(const DataTypes::StringView& source, Diagnostic& diagnostic)
        :   Lexer(source.Data(), source.Size(), diagnostic){}

    bool Lexer::AtEnd() const{
        return this->_current >= this->_end;
    }

    char Lexer::Peek(const Int offset) const{
        return (this->_end - this->_current > offset)
                   ? this->_current[offset]
                   : '\0';
    }

    void Lexer::ConsumeNewLine(){
        ++this->_line;
        this->_lineStart = this->_current + 1;
    }

    void Lexer::SkipWhitespace(){
        while (!this->AtEnd()){
            switch (*this->_current){
                case '\n':
                    this->ConsumeNewLine();
                    ++this->_current;
                    break;

                case ' ':
                case '\t':
                case '\r':
                    ++this->_current;
                    break;

                default:
                    return;
            }
        }
    }

    Token Lexer::Emit(const TokenType type, const char* textBegin, const Int textLength) const{
        Token token{};

        token.type = type;
        token.text = textBegin;
        token.length = textLength;
        token.line = this->_tokenLine;
        token.column = this->_tokenColumn;

        return token;
    }

    Token Lexer::Fail(
        const DataTypes::StringView& message,
        const char* textBegin,
        const Int textLength
    ){
        this->_diagnostic->Raise(
            message, this->_tokenLine, this->_tokenColumn,
            DataTypes::StringView(textBegin, textLength)
        );

        return this->Emit(TokenType::Invalid, textBegin, textLength);
    }

    Token Lexer::Next(){
        this->SkipWhitespace();

        this->_tokenLine = this->_line;
        this->_tokenColumn = static_cast<UnsignedInt>(this->_current - this->_lineStart) + 1;

        if (this->AtEnd())
            return this->Emit(TokenType::EndOfFile, this->_current, 0);

        const char* tokenStart = this->_current;
        const char current = *this->_current;

        //N'...' has to be tested before the identifier path claims the leading N.
        if ((current == 'N' || current == 'n') && this->Peek(1) == '\''){
            ++this->_current;
            return this->ScanString(TokenType::UnicodeStringLiteral);
        }

        if (Lexer::IsIdentifierStart(current))
            return this->ScanIdentifier(tokenStart);

        if (Lexer::IsDigit(current))
            return this->ScanNumber(tokenStart);

        if (current == '\'')
            return this->ScanString(TokenType::StringLiteral);

        if (current == '[')
            return this->ScanQuotedIdentifier();

        if (current == '@')
            return this->ScanVariable(tokenStart);

        return this->ScanOperator(tokenStart);
    }

    Token Lexer::ScanIdentifier(const char* tokenStart){
        while (!this->AtEnd() && Lexer::IsIdentifierPart(*this->_current))
            ++this->_current;

        const auto length = static_cast<Int>(this->_current - tokenStart);

        return this->Emit(
            LookupKeyword(DataTypes::StringView(tokenStart, length)),
            tokenStart, length
        );
    }

    Token Lexer::ScanQuotedIdentifier(){
        ++this->_current; //consume '['

        const char* textBegin = this->_current;

        while (!this->AtEnd() && *this->_current != ']'){
            if (*this->_current == '\n')
                this->ConsumeNewLine();

            ++this->_current;
        }

        const auto length = static_cast<Int>(this->_current - textBegin);

        if (this->AtEnd())
            return this->Fail("unterminated quoted identifier, expected a closing ']'", textBegin, length);

        ++this->_current; //consume ']'

        if (length == 0)
            return this->Fail("empty quoted identifier", textBegin, 0);

        //A quoted name is always a name, never a keyword. That is the point of quoting it.
        return this->Emit(TokenType::Identifier, textBegin, length);
    }

    Token Lexer::ScanVariable(const char* tokenStart){
        ++this->_current; //consume '@'

        if (this->AtEnd() || !Lexer::IsIdentifierStart(*this->_current))
            return this->Fail("expected a variable name after '@'", tokenStart, 1);

        while (!this->AtEnd() && Lexer::IsIdentifierPart(*this->_current))
            ++this->_current;

        //The '@' stays part of the text: variables are named with it everywhere else
        //in the pipeline.
        return this->Emit(TokenType::Variable, tokenStart, static_cast<Int>(this->_current - tokenStart));
    }

    Token Lexer::ScanNumber(const char* tokenStart){
        while (!this->AtEnd() && Lexer::IsDigit(*this->_current))
            ++this->_current;

        //A '.' only continues the number when a digit follows, so both "t1.col" and a
        //trailing "1." keep the dot as a token of its own.
        if (this->Peek() != '.' || !Lexer::IsDigit(this->Peek(1)))
            return this->Emit(TokenType::IntegerLiteral, tokenStart, static_cast<Int>(this->_current - tokenStart));

        ++this->_current; //consume '.'

        while (!this->AtEnd() && Lexer::IsDigit(*this->_current))
            ++this->_current;

        return this->Emit(TokenType::DecimalLiteral, tokenStart, static_cast<Int>(this->_current - tokenStart));
    }

    Token Lexer::ScanString(const TokenType type){
        const char* quoteStart = this->_current;
        ++this->_current; //consume the opening quote

        const char* textBegin = this->_current;

        while (!this->AtEnd() && *this->_current != '\''){
            if (*this->_current == '\\'){
                ++this->_current; //consume the backslash

                if (this->AtEnd())
                    break;

                //A backslash escapes whatever follows it, including a quote.
                if (*this->_current == '\n')
                    this->ConsumeNewLine();

                ++this->_current;
                continue;
            }

            if (*this->_current == '\n')
                this->ConsumeNewLine();

            ++this->_current;
        }

        if (this->AtEnd())
            return this->Fail(
                "unterminated string literal, expected a closing quote",
                quoteStart, static_cast<Int>(this->_current - quoteStart)
            );

        const auto length = static_cast<Int>(this->_current - textBegin);
        ++this->_current; //consume the closing quote

        //Quotes are stripped here; escape sequences are left for the parser to resolve.
        return this->Emit(type, textBegin, length);
    }

    Token Lexer::ScanOperator(const char* tokenStart){
        const char current = *this->_current;
        ++this->_current;

        switch (current){
            case '(': return this->Emit(TokenType::LeftParen,  tokenStart, 1);
            case ')': return this->Emit(TokenType::RightParen, tokenStart, 1);
            case ',': return this->Emit(TokenType::Comma,      tokenStart, 1);
            case ';': return this->Emit(TokenType::Semicolon,  tokenStart, 1);
            case '.': return this->Emit(TokenType::Dot,        tokenStart, 1);
            case '+': return this->Emit(TokenType::Plus,       tokenStart, 1);
            case '*': return this->Emit(TokenType::Star,       tokenStart, 1);
            case '/': return this->Emit(TokenType::Slash,      tokenStart, 1);
            case '%': return this->Emit(TokenType::Percent,    tokenStart, 1);
            case '=': return this->Emit(TokenType::Equal,      tokenStart, 1);

            case '-':
                if (this->Peek() != '>')
                    return this->Emit(TokenType::Minus, tokenStart, 1);

                //"->>" beats "->", longest match wins.
                if (this->Peek(1) == '>'){
                    this->_current += 2;
                    return this->Emit(TokenType::JsonScalarAccessor, tokenStart, 3);
                }

                ++this->_current;
                return this->Emit(TokenType::JsonObjectAccessor, tokenStart, 2);

            case '<':
                if (this->Peek() == '='){
                    ++this->_current;
                    return this->Emit(TokenType::LessEqual, tokenStart, 2);
                }

                if (this->Peek() == '>'){
                    ++this->_current;
                    return this->Emit(TokenType::NotEqual, tokenStart, 2);
                }

                return this->Emit(TokenType::Less, tokenStart, 1);

            case '>':
                if (this->Peek() == '='){
                    ++this->_current;
                    return this->Emit(TokenType::GreaterEqual, tokenStart, 2);
                }

                return this->Emit(TokenType::Greater, tokenStart, 1);

            case '!':
                if (this->Peek() == '='){
                    ++this->_current;
                    return this->Emit(TokenType::NotEqual, tokenStart, 2);
                }

                return this->Fail("expected '=' after '!'", tokenStart, 1);

            default:
                return this->Fail("unexpected character", tokenStart, 1);
        }
    }

    bool Tokenize(
        const DataTypes::StringView& source,
        DataStructures::PolymorphicArray<Token>& tokens,
        Diagnostic& diagnostic
    ){
        //Roughly a token every four bytes of SQL, enough to keep the array from regrowing.
        tokens.Reserve(tokens.Size() + source.Size() / 4 + 1);

        Lexer lexer(source, diagnostic);

        for (;;){
            const Token token = lexer.Next();
            tokens.Push(token);

            if (token.type == TokenType::EndOfFile)
                return true;

            if (token.type == TokenType::Invalid){
                //Keep the array EndOfFile terminated even on failure, so anything that
                //inspects the tokens afterwards can still walk them safely.
                Token terminator = token;
                terminator.type = TokenType::EndOfFile;
                terminator.length = 0;

                tokens.Push(terminator);
                return false;
            }
        }
    }
}
