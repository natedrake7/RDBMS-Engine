#include <system_error>
#include <CoreEngine/BufferPool/File.h>
#include <Systemic/DataTypes/StringView.h>

#include <CoreEngine/BufferPool/StorageLayout.h>
#include <Systemic/Functions/MathFunctions.h>

#ifdef _WIN32
    #define WIN32_LEAN_AND_MEAN
    #define NOMINMAX
    #define byte win_byte_override
    #include <windows.h>
    #undef byte
#else
    #include <cerrno>
    #include <fcntl.h>
    #include <sys/uio.h>
    #include <unistd.h>
#endif

namespace Storage::FileIO{
#ifdef _WIN32
    namespace{
        inline constexpr DWORD WINDOWS_MEMORY_PAGE = 4096;

        static_assert(
            CoreEngine::StorageLayout::PAGE_SIZE % WINDOWS_MEMORY_PAGE == 0,
            "PageSize modulo windows memory page must be 0 for read scatter to work"
        );

        [[noreturn]] void ThrowLastError(const char* what){
            throw std::system_error(static_cast<Int>(GetLastError()), std::system_category(), what);
        }

        struct ThreadIOEvent{
            HANDLE _event;
            ThreadIOEvent()
                : _event(CreateEventW(nullptr, TRUE, FALSE, nullptr)){}
            ~ThreadIOEvent(){
                if (this->_event != nullptr)
                    CloseHandle(this->_event);
            }

            [[nodiscard]] inline bool HasEvent() const{
                return this->_event != nullptr;
            }
        };

       [[nodiscard]] inline HANDLE ThreadEvent(){
            thread_local ThreadIOEvent event;
            if (!event.HasEvent())
                ThrowLastError("ThreadEvent");
            return event._event;
        }

        [[nodiscard]] inline OVERLAPPED OverlappedAt(const UnsignedBigInt offset){
            return {
                .Offset = static_cast<DWORD>(offset),
                .OffsetHigh = static_cast<DWORD>(offset >> 32),
                .hEvent = ThreadEvent()
            };
        }

        [[nodiscard]] DWORD FinishIO(
            const HANDLE handle,
            const BOOL started,
            OVERLAPPED& overlapped,
            const char* what
        ){
            if (!started){
                const auto error = GetLastError();
                if (error == ERROR_HANDLE_EOF)
                    return 0;
                if (error != ERROR_IO_PENDING)
                    ThrowLastError(what);
            }

           DWORD transferred = 0;
           if (!GetOverlappedResult(handle, &overlapped, &transferred, TRUE)){
                if (GetLastError() == ERROR_HANDLE_EOF)
                    return transferred;
               ThrowLastError(what);
           }

           return transferred;
       }
    }
#else

    [[noreturn]] void ThrowErrno(const char* what){
        throw std::system_error(errno, std::generic_category(), what);
    }
#endif

    native_file_t Open(
        const DataTypes::StringView& pathView,
        const FileOpenMode mode,
        const FileOpenFlags flags
    ){
        const auto path = pathView.ToPath();

        if (mode != FileOpenMode::OpenExisting && path.has_parent_path())
            std::filesystem::create_directories(path.parent_path());

#ifdef _WIN32
        DWORD access;
        switch (mode){
        case FileOpenMode::OpenExisting:
            access = OPEN_EXISTING;
            break;
        case FileOpenMode::CreateNew:
            access = CREATE_NEW;
            break;
        case FileOpenMode::OpenOrCreate:
            access = OPEN_ALWAYS;
            break;
        default:
            std::unreachable();
        }

        DWORD attributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;

        if (HasFlag(flags, FileOpenFlags::Direct))
            attributes |= FILE_FLAG_NO_BUFFERING;
        if (HasFlag(flags, FileOpenFlags::WriteThrough))
            attributes |= FILE_FLAG_WRITE_THROUGH;
        if (HasFlag(flags, FileOpenFlags::Async))
            attributes |= FILE_FLAG_OVERLAPPED;

        const auto handle = CreateFileW(
            path.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,                 // backups may read; nobody else may write or delete
            nullptr,
            access,
            attributes,
            nullptr
        );

        if (handle == INVALID_HANDLE_VALUE)
            ThrowLastError("FileIO:Open");

        return handle;
#else
        int openFlags = O_RDWR | O_CLOEXEC;
        if (mode == FileOpenMode::CreateNew)
            openFlags |= O_CREAT | O_EXCL;
        else if (mode == FileOpenMode::OpenOrCreate)
            openFlags |= O_CREAT;

#ifdef O_DIRECT
        if (HasFlag(flags, ::Storage::FileOpenFlags::Direct))
            openFlags |= O_DIRECT;
#endif
        if (HasFlag(flags, ::Storage::FileOpenFlags::WriteThrough))
            openFlags |= O_DSYNC;

        const int fd = ::open(path.c_str(), openFlags, 0644);
        if (fd < 0)
            ThrowErrno("FileIO::Open");

        return fd;
#endif
    }

    void Close(native_file_t file){
        if (file == INVALID_NATIVE_FILE)
            return;

#ifdef _WIN32
        CloseHandle(file);
#else
        ::close(file);
#endif
    }

    UnsignedBigInt ReadScatter(
        const native_file_t file,
        const std::span<object_t* const> pages,
        const UnsignedBigInt offset
    ){
        assert(!pages.empty() && pages.size() <= CoreEngine::StorageLayout::EXTENT_SIZE);
#ifdef _WIN32
        constexpr auto ENTRIES_PER_PAGE = CoreEngine::StorageLayout::PAGE_SIZE / WINDOWS_MEMORY_PAGE;

        FILE_SEGMENT_ELEMENT segments[CoreEngine::StorageLayout::EXTENT_SIZE * ENTRIES_PER_PAGE + 1] = {};

        UnsignedBigInt counter = 0;
        for (auto* page: pages){
            assert(reinterpret_cast<std::uintptr_t>(page) % WINDOWS_MEMORY_PAGE == 0);
            for (auto part = 0; part < ENTRIES_PER_PAGE; part++){
                auto* segment = segments + counter;
                segment->Buffer = PtrToPtr64(page + part * WINDOWS_MEMORY_PAGE);
                counter++;
            }
        }

        auto overlapped = OverlappedAt(offset);
        const auto started = ReadFileScatter(
            file,
            segments,
            pages.size() * CoreEngine::StorageLayout::PAGE_SIZE,
            nullptr,
            &overlapped
        );

        const auto bytes = FinishIO(file, started, overlapped, "FileIO:ReadScatter");
        return bytes / CoreEngine::StorageLayout::PAGE_SIZE;
#else
        iovec vectors[EXTENT_SIZE];
        for (size_t i = 0; i < pages.size(); ++i)
            vectors[i] = iovec{
            pages[i],
            PAGE_SIZE
        };

        const auto total = static_cast<UnsignedBigInt>(pages.size()) * PAGE_SIZE;
        UnsignedBigInt done = 0;
        while (done < total){
            // A short read can stop mid-buffer: continue from the first unfilled buffer
            const auto first = static_cast<size_t>(done / PAGE_SIZE);
            const auto skip  = static_cast<size_t>(done % PAGE_SIZE);
            vectors[first].iov_base = static_cast<char*>(pages[first]) + skip;
            vectors[first].iov_len  = PAGE_SIZE - skip;

            const auto n = ::preadv(
                file,
                vectors + first,
                static_cast<Int>(pages.size() - first),
                static_cast<off_t>(offset + done)
            );

            if (n < 0){
                if (errno == EINTR)
                    continue;
                ThrowErrno("FileIO::ReadScatter");
            }
            if (n == 0)
                break;                                     // end of file
            done += static_cast<UnsignedBigInt>(n);
        }
        return static_cast<UnsignedInt>(done / PAGE_SIZE);
#endif
    }

    UnsignedBigInt Read(
        native_file_t file,
        void* data,
        const UnsignedBigInt size,
        const UnsignedBigInt offset
    ){
        auto* bytes = static_cast<char*>(data);
        UnsignedBigInt done = 0;

        while (done < size){
#ifdef _WIN32
            auto overlapped = OverlappedAt(offset+ done);
            const auto chunk = static_cast<DWORD>(Math::Min<UnsignedBigInt>(size - done, MAXDWORD));
            const auto started = ReadFile(file, bytes + done, chunk, nullptr, &overlapped);
            const auto transferred = FinishIO(file, started, overlapped, "FileIO:Read");
#else
            const auto transferred = ::pread(file, bytes + done, size - done, static_cast<off_t>(offset + done));
            if (transferred < 0){
                if (errno == EINTR)
                    continue;
                ThrowErrno("FileIO::Read");
            }
#endif
            if (transferred == 0)
                break;                                   // end of file

            done += static_cast<UnsignedBigInt>(transferred);
        }

        return done;
    }

    void Write(
        native_file_t file,
        const void* data,
        const UnsignedBigInt size,
        const UnsignedBigInt offset
    ){
        const auto* bytes = static_cast<const char*>(data);
        UnsignedBigInt done = 0;

        while (done < size){
#ifdef _WIN32
            auto overlapped = OverlappedAt(offset + done);
            const auto chunk = static_cast<DWORD>(Math::Min<UnsignedBigInt>(size - done, MAXDWORD));
            const auto started = WriteFile(file, bytes + done, chunk, nullptr, &overlapped);
            const auto transferred = FinishIO(file, started, overlapped, "FileIO:Write");
#else
            const auto transferred = ::pwrite(file, bytes + done, size - done, static_cast<off_t>(offset + done));
            if (transferred < 0){
                if (errno == EINTR)
                    continue;
                ThrowErrno("FileIO::Write");
            }
#endif
            if (transferred == 0)
                throw std::runtime_error("FileIO::Write: no progress");

            done += static_cast<UnsignedBigInt>(transferred);
        }
    }

    void Sync(native_file_t file){
#ifdef _WIN32
        if (!FlushFileBuffers(file))
            ThrowLastError("FileIO::Sync");
#else
        if (::fdatasync(file) != 0)
            ThrowErrno("FileIO::Sync");
#endif
    }
}
