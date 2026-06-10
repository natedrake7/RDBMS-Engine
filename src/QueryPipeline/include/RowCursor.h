#pragma once
#include <iosfwd>

#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace QueryPipeline{
    namespace PhysicalPlan
    {
        struct VectorBatch;
    }

    struct RowCursor{
        const PhysicalPlan::VectorBatch* _batch;

        explicit RowCursor(const PhysicalPlan::VectorBatch* batch);

        friend std::ostream& operator<<(std::ostream& os, const RowCursor& cursor);

        void PrintRows(std::ostream& os)const;
        void PrintColumn(std::ostream& os, Int rowIndex, Int columnIndex)const;
    };
}
