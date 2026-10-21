#pragma once
#include <atomic>

#include <Systemic/DataTypes/DataTypes.h>

#ifdef _WIN32
    using native_file_t = void*;    // HANDLE
    inline const auto INVALID_NATIVE_FILE = reinterpret_cast<native_file_t>(static_cast<std::intptr_t>(-1));
#else
    using native_file_t = int;
    inline constexpr native_file_t INVALID_NATIVE_FILE = -1;
#endif

namespace Storage{
    enum class FileOpenMode: UnsignedTinyInt{
        OpenExisting = 0,
        CreateNew = 1,
        OpenOrCreate = 2
    };

    enum class FileOpenFlags: UnsignedTinyInt{
        None = 0,
        Direct = 1 << 0,
        WriteThrough = 1 << 1,
        Async = 1 << 2,
    };

    [[nodiscard]] inline constexpr FileOpenFlags operator|(const FileOpenFlags lhs, const FileOpenFlags rhs){
        return static_cast<FileOpenFlags>(std::to_underlying(lhs) | std::to_underlying(rhs));
    }

    [[nodiscard]] inline constexpr bool HasFlag(const FileOpenFlags flags, const FileOpenFlags flag){
        return (std::to_underlying(flags) & std::to_underlying(flag)) != 0;
    }

    namespace FileIO{
        [[nodiscard]] native_file_t Open(
            const DataTypes::StringView& pathView,
            FileOpenMode mode,
            FileOpenFlags flags
        );

        void Close(native_file_t file);

        [[nodiscard]] UnsignedBigInt ReadScatter(
            native_file_t file,
            std::span<object_t* const> pages,
            UnsignedBigInt offset
        );

        [[nodiscard]] UnsignedBigInt Read(
            native_file_t file,
            void* data,
            UnsignedBigInt size,
            UnsignedBigInt offset
        );

        void Write(
            native_file_t file,
            const void* data,
            UnsignedBigInt size,
            UnsignedBigInt offset
        );

        void Sync(native_file_t file);
    }

    struct File{
        static inline constexpr Int MAX_PATH_SIZE = 512;

        char _path[MAX_PATH_SIZE];
        std::atomic<native_file_t> _handle;

        File()
            : _path{}, _handle(INVALID_NATIVE_FILE){}

        [[nodiscard]] inline bool IsOpen()const{
            return this->_handle.load(std::memory_order_acquire) != INVALID_NATIVE_FILE;
        }
    };
}
