#include "FunctionSizeEvaluator.h"

namespace Expressions::Functions {
  size_t FunctionSizeEvaluator::GetDate(const std::vector<Expression*>& expressions) { return sizeof(int64_t); }

  size_t FunctionSizeEvaluator::NewGuid(const std::vector<Expression*>& expressions) { return 16; }

  size_t FunctionSizeEvaluator::Concat(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Length(const std::vector<Expression*>& expressions) { return sizeof(int32_t); }

  size_t FunctionSizeEvaluator::AsciiValue(const std::vector<Expression*>& expressions) { return sizeof(int32_t); }

  size_t FunctionSizeEvaluator::Char(const std::vector<Expression*>& expressions) { return 1; }

  size_t FunctionSizeEvaluator::CharIndex(const std::vector<Expression*>& expressions) { return sizeof(int32_t); }

  size_t FunctionSizeEvaluator::Lower(const std::vector<Expression*>& expressions) {
    if (expressions.empty())
      return -1;

    // return expressions.front()->

    return 0;
  }

  size_t FunctionSizeEvaluator::Upper(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Trim(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::TrimLeft(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::TrimRight(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Replace(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Substr(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Left(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Right(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Reverse(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::Space(const std::vector<Expression*>& expressions) { return 0; }

  size_t FunctionSizeEvaluator::GetFunctionReturnSize(const Constants::FunctionType &functionType, const std::vector<Expression *> &expressions) {
   if (!FunctionReturnSizeDictionary.Contains(functionType))
    return -1;

   return FunctionReturnSizeDictionary.Get(functionType)(expressions);
  }
}