#pragma once

#include <string>
#include "../Dictionary/Dictionary.h"
#include "../../Database/Constants.h"
#include "../Expressions/Expression.h"

#include <functional>
namespace Expressions::Functions {
    class FunctionSizeEvaluator {
         [[nodiscard]] static size_t GetDate(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t NewGuid(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Concat(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Length(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t AsciiValue(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Char(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t CharIndex(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Lower(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Upper(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Trim(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t TrimLeft(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t TrimRight(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Replace(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Substr(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Left(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Right(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Reverse(const std::vector<Expression*>& expressions);
         [[nodiscard]] static size_t Space(const std::vector<Expression*>& expressions);

        static inline const Dictionary<Constants::FunctionType, std::function<size_t(const std::vector<Expression*>& expressions)>> FunctionReturnSizeDictionary{
                {Constants::FunctionType::GetDate,      &FunctionSizeEvaluator::GetDate},
                {Constants::FunctionType::NewGuid,      &FunctionSizeEvaluator::NewGuid},
                {Constants::FunctionType::Concat,       &FunctionSizeEvaluator::Concat},
                {Constants::FunctionType::Length,       &FunctionSizeEvaluator::Length},
                {Constants::FunctionType::AsciiValue,   &FunctionSizeEvaluator::AsciiValue},
                {Constants::FunctionType::Char,         &FunctionSizeEvaluator::Char},
                {Constants::FunctionType::CharIndex,    &FunctionSizeEvaluator::CharIndex},
                {Constants::FunctionType::Lower,        &FunctionSizeEvaluator::Lower},
                {Constants::FunctionType::Upper,        &FunctionSizeEvaluator::Upper},
                {Constants::FunctionType::Trim,         &FunctionSizeEvaluator::Trim},
                {Constants::FunctionType::TrimLeft,     &FunctionSizeEvaluator::TrimLeft},
                {Constants::FunctionType::TrimRight,    &FunctionSizeEvaluator::TrimRight},
                {Constants::FunctionType::Replace,      &FunctionSizeEvaluator::Replace},
                {Constants::FunctionType::Substr,       &FunctionSizeEvaluator::Substr},
                {Constants::FunctionType::Left,         &FunctionSizeEvaluator::Left},
                {Constants::FunctionType::Right,        &FunctionSizeEvaluator::Right},
                {Constants::FunctionType::Reverse,      &FunctionSizeEvaluator::Reverse},
                {Constants::FunctionType::Space,        &FunctionSizeEvaluator::Space}
        };

        public:
          [[nodiscard]] static size_t GetFunctionReturnSize(const Constants::FunctionType& functionType, const std::vector<Expression*>& expressions);
    };
}

