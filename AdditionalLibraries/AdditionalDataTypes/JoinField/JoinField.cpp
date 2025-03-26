#include "JoinField.h"

JoinField::JoinField(const Field& firstTableField, const Field& secondTableField)
{
    this->firstTableCondition = firstTableField;
    this->secondTableCondition = secondTableField;
}

const Field& JoinField::GetFirstTableCondition() const { return this->firstTableCondition; };

const Field& JoinField::GetSecondTableCondition() const  { return this->firstTableCondition; };