#pragma once
#include "../Page.h"


namespace Pages{
  template<typename T = Page>
  class PageGuard {
    T* _page;

    void Release() {
      if (this->_page)
        this->_page->DecreasePinCount();
    }

    public:
      PageGuard() {
        this->_page = nullptr;
      }

      explicit PageGuard(T* page) : _page(page) {
          this->_page->IncreasePinCount();
      }

      PageGuard(const PageGuard& other){
        this->_page = other._page;

        if (this->_page)
            _page->IncreasePinCount();
      }

    // Move constructor
      PageGuard(PageGuard&& other) noexcept{
        this->_page = other._page;
        other._page = nullptr;
      }

    //Move assignment operator
      PageGuard& operator=(PageGuard&& other) noexcept {
        if (this == &other)
          return *this;

        this->Release();
        this->_page = other._page;
        other._page = nullptr;

        return *this;
      }

    // Copy assignment operator
      PageGuard& operator=(const PageGuard& other) noexcept {
        if (this != &other) {
          this->Release();
          this->_page = other._page;
        }

        return *this;
      }

      ~PageGuard() {
        this->Release();
      }

      [[nodiscard]] bool IsValid() const { return this->_page != nullptr; }

      T* operator->() { return this->_page; }
      const T* operator->() const { return this->_page; }

      T& operator*() { return *this->_page; }
      const T& operator*() const { return *this->_page; }

      T* Get() { return this->_page; }
      const T* Get() const { return this->_page; }
  };
}