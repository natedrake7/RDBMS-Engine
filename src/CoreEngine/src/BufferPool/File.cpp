#include <system_error>
#include <CoreEngine/BufferPool/File.h>
#include <Systemic/DataTypes/StringView.h>
#include <winsock2.h>

namespace Storage::FileIO{
#ifdef _WIN32
    namespace{
        OVERLAPPED AtOffset(const UnsignedBigInt offset){
            OVERLAPPED overlapped{};
            overlapped.Offset = static_cast<DWORD>(offset);
            overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
            return overlapped;
        }

        [[noreturn]] void ThrowLastError(const char* what){
            throw std::system_error(static_cast<Int>(GetLastError()), std::system_category(), what);
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
            access = CREATE_NEW;
            break;
        case FileOpenMode::CreateNew:
            access = OPEN_EXISTING;
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
        if (HasFlag(flags, FileOpenFlags::Direct))
            openFlags |= O_DIRECT;
#endif
        if (HasFlag(flags, FileOpenFlags::WriteThrough))
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
            auto overlapped = AtOffset(offset + done);
            const auto chunk = static_cast<DWORD>(std::min<UnsignedBigInt>(size - done, MAXDWORD));
            DWORD transferred = 0;

            if (!ReadFile(file, bytes + done, chunk, &transferred, &overlapped)){
                if (GetLastError() == ERROR_HANDLE_EOF)
                    break;
                ThrowLastError("FileIO::Read");
            }
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
            auto overlapped = AtOffset(offset + done);
            const auto chunk = static_cast<DWORD>(std::min<UnsignedBigInt>(size - done, MAXDWORD));
            DWORD transferred = 0;

            if (!WriteFile(file, bytes + done, chunk, &transferred, &overlapped))
                ThrowLastError("FileIO::Write");
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
