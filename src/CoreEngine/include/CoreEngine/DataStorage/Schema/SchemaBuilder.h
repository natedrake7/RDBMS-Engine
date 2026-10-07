#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine::Catalog{
    struct TableHeader;
}

namespace CoreEngine::Schemas{
    struct TableSchema;

    [[nodiscard]] const TableSchema* BuildTableSchema(
        const Catalog::TableHeader& table,
        schema_version_t version
    );
}
