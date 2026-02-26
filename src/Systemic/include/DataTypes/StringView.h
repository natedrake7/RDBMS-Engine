#pragma once
#include "DataTypes.h"

namespace DataTypes{
    class StringView{
        object_t* _data;
        Int size;

        public:
        /**
             *
             * @param data Non-owning pointer of the actual data
             * @param size The size of the string view in bytes (not including null terminator, if any).
             * The string view can contain null characters within it and is not required to be null-terminated.
        */
        StringView(object_t* data, Int size);

        [[nodiscard]] Int GetSize() const;
        [[nodiscard]] object_t* GetData() const;
        friend std::ostream& operator<<(std::ostream& os, const StringView& sv);
    };
}
