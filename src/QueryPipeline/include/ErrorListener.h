#pragma once
#include <BaseErrorListener.h>
#include <Recognizer.h>

namespace QueryPipeline {
  class ErrorListener final : public antlr4::BaseErrorListener{
    public:
    ErrorListener() = default;
    ~ErrorListener()override = default;
    static void SyntaxError(antlr4::Recognizer *recognizer, antlr4::Token *offendingSymbol,
                     size_t line, size_t charPositionInLine,
                     const std::string &msg, const std::exception_ptr& e);
  };

  class SyntaxError final : public std::runtime_error {
    public:
      explicit SyntaxError(const std::string_view msg, const std::string& posMsg)
        : std::runtime_error(std::string(msg) + posMsg) {}
  };
}