#include <Systemic/DataTypes/Decimal.h>

#include <Systemic/DataTypes/StringValue.h>

namespace DataTypes {
    String Decimal::ToString(const ::Memory::IAllocator* allocator, const Int scale) const{
        const auto buffer = this->ToStringBuffer(scale);
        return String(buffer.Data(), static_cast<data_size_t>(buffer.Size()), allocator);
    }

    StringValue Decimal::ToStringValue(const ::Memory::IAllocator* allocator, const Int scale) const{
        const auto buffer = this->ToStringBuffer(scale);
        return StringValue::Create(allocator, buffer.Data(), static_cast<data_size_t>(buffer.Size()));
    }
}


