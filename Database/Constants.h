#pragma once

constexpr size_t PAGE_SIZE = 8 * 1024;
constexpr size_t MAX_NUMBER_OF_PAGES = 100;
constexpr size_t EXTENT_SIZE = 8;
constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;