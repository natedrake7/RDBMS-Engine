#include "../../include/DataTypes/StringView.h"

#include <ostream>

namespace DataTypes{
    StringView::StringView(object_t* data, const Int size){
        this->_data = data;
        this->size = size;
    }

    Int StringView::GetSize() const{ return this->size; }

    object_t* StringView::GetData() const{ return this->_data; }

    std::ostream& operator<<(std::ostream& os, const StringView& sv){
        os.write(reinterpret_cast<const char*>(sv._data), sv.size);
        return os;
    }
}
