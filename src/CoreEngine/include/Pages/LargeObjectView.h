#pragma once
#include "PageView.h"

namespace Pages{
    class LargeObjectView final : public PageView{
        public:
            LargeObjectView();
            explicit LargeObjectView(Frame* framePtr);

            LargeObjectView(LargeObjectView&& other) noexcept;
            LargeObjectView& operator=(LargeObjectView&& other) noexcept;

            [[nodiscard]] page_size_t GetObjectSize() const;
            [[nodiscard]] page_id_t GetNextPageId() const;

            void SetObjectSize(page_size_t size) const;
            void SetNextPageId(page_id_t nextPageId) const;

            void SetData(const object_t* object, page_size_t size) const;
    };
}
