#include "../../include/DataTypes/StringValue.h"
#include "../../include/DataTypes/String.h"

namespace DataTypes{
    StringValue::StringValue(String& str){
        this->_value._inlineVal._size = str.Size();

        if (this->_value._inlineVal._size <= INLINE_SIZE)
            std::memcpy(this->_value._inlineVal._data, str.Data(), str.Size());
        else{
            this->_value._external._data = str.Data();
            std::memcpy(this->_value._external._prefix, this->_value._external._data, PREFIX_SIZE);
        }
    }

    StringValue StringValue::Create(const Memory::IAllocator* allocator, const StringView& strView){
        const auto* data = strView.Data();
        const auto size = strView.Size();
        return (size <= INLINE_SIZE)
            ? StringValue(data, size)
            : StringValue(allocator, data, size);
    }

    StringValue StringValue::MoveFromString(String& str){
        return StringValue(str);
    }
}
