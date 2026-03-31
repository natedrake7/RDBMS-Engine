#include "../../include/DataTypes/Decimal.h"

namespace DataTypes {
    String Decimal::ToString(const ::Memory::IAllocator* allocator) const{
        const auto isPositive = this->IsPositive();

        String result(allocator, this->_data.Size());

        if (!isPositive) result[0] = '-';

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex() + !isPositive;

        Int strCounter = !isPositive;
        for (int i = DECIMAL_DIGITS_START_INDEX; i < this->_data.Size(); i++){
            if (strCounter == fractionIndex){
                result[strCounter] = '.';

                i--;
                strCounter++;

                continue;
            }

            result[strCounter] = static_cast<char>((this->_data[i] >> 4) & 0x0F);
            strCounter++;
        }

        if (result.Last() == '0')
            result.Pop();

        return result;
    }

    double Decimal::ToDouble() const{
        // return std::stod(this->ToString());
    }
}


