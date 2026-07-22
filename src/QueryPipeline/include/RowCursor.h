#pragma once
#include <iosfwd>

#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine{
    struct DataChunk;
}

namespace QueryPipeline{

    struct RowCursor{
        const CoreEngine::DataChunk* _batch;

        explicit RowCursor(const CoreEngine::DataChunk* batch);

        friend std::ostream& operator<<(std::ostream& os, const RowCursor& cursor);

        void PrintRows(std::ostream& os)const;
        void PrintColumn(std::ostream& os, Int rowIndex, Int columnIndex)const;
    };
}
