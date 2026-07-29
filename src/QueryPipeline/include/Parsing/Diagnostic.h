#pragma once
#include "../../../Systemic/include/DataTypes/StringView.h"

namespace QueryPipeline::Parsing{

    /**
     * A syntax error, reported without throwing so the parse path stays allocation and
     * exception free. The first error wins: once raised, later calls are ignored, since
     * everything after the first bad token is noise.
     *
     * @c message points at a static string literal and @c near at the offending lexeme
     * inside the query buffer, so neither outlives the query being parsed. Callers turn
     * these into an arena owned message at the boundary.
     */
    struct Diagnostic {
        DataTypes::StringView message;
        DataTypes::StringView near;

        //Set when the parser wanted one specific token, so the message can read
        //"expected ')'" without formatting anything on the error path.
        DataTypes::StringView expected;

        UnsignedInt line;
        UnsignedInt column;

        bool hasError;

        Diagnostic()
            : message(), near(), expected(), line(0), column(0), hasError(false){}

        void Raise(
            const DataTypes::StringView& errorMessage,
            const UnsignedInt errorLine,
            const UnsignedInt errorColumn,
            const DataTypes::StringView& offendingText = DataTypes::StringView(),
            const DataTypes::StringView& expectedToken = DataTypes::StringView()
        ){
            if (this->hasError)
                return;

            this->message = errorMessage;
            this->near = offendingText;
            this->expected = expectedToken;
            this->line = errorLine;
            this->column = errorColumn;
            this->hasError = true;
        }
    };
}
