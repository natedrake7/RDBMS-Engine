#pragma once
#include "../../../Systemic/include/DataStructures/SortedDictionary.h"
#include "../../../Systemic/include/DataTypes/Guid.h"
#include "../../../Systemic/include/DataTypes/DateTime.h"
#include "../../../Systemic/include/DataTypes/BoundVariable.h"
#include "Security.h"


namespace QueryPipeline {
    class Cursor;
}

namespace Network {
    struct Session {
        session_id_t sessionId;

        const Security::User* user;

        DataTypes::DateTime createdAt;
        DataTypes::DateTime lastActive;

        Int databaseId;

        SortedDictionary<UnsignedSmallInt, QueryPipeline::Cursor*> cursors;
        Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>> variables;

        UnsignedSmallInt nextCursorId;

        transaction_id_t transactionId;

        //add permissions later and session variables etc
        explicit Session(const Security::User* user)
            :   sessionId(INVALID_SESSION_ID), user(user),
                createdAt(DataTypes::DateTime::Now()), lastActive(DataTypes::DateTime::Now()),
                databaseId(0), nextCursorId(0),
                transactionId(0) {}

        Session() = default;
        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        Session(Session&&)noexcept = default;
        Session& operator=(Session&&)noexcept = default;
    };
}