#include <Systemic/DataTypes/Guid.h>

#include <array>
#include <atomic>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>

#include <Systemic/DataTypes/StringValue.h>
#include <Systemic/DataTypes/StringView.h>

namespace DataTypes {
    String Guid::ToString(const ::Memory::IAllocator* allocator) const {
        const auto buffer = this->ToStringBuffer();
        return String(buffer.Data(), GUID_STRING_SIZE, allocator);
    }

    StringValue Guid::ToStringValue(const Memory::IAllocator* allocator) const{
        auto buffer = this->ToStringBuffer();
        return StringValue::Create(allocator, buffer.Data(), GUID_STRING_SIZE);
    }

    std::ostream & operator<<(std::ostream &os, const Guid &guid){
        const auto buffer = guid.ToStringBuffer();
        os.write(buffer.Data(), GUID_STRING_SIZE);
        return os;
    }

    std::mt19937_64& Guid::ThreadGenerator(){
        thread_local std::mt19937_64 generator = []{
            std::random_device device;
            std::array<std::uint32_t, 8> seed{};
            for (auto& word : seed)
                word = device();

            std::seed_seq sequence(seed.begin(), seed.end());
            return std::mt19937_64(sequence);
        }();
        return generator;
    }

    Guid Guid::NewGuid(){
        auto& generator = ThreadGenerator();
        const std::uint64_t words[2] = {generator(), generator()};

        Guid result;
        std::memcpy(result._data, words, GUID_SIZE);

        result._data[6] = static_cast<UnsignedTinyInt>((result._data[6] & 0x0F) | 0x40);   // version 4 (random)
        result._data[8] = static_cast<UnsignedTinyInt>((result._data[8] & 0x3F) | 0x80);   // RFC 4122 variant

        return result;
    }

    Guid Guid::NewSequentialGuid(){
        // 48-bit Unix milliseconds + 12-bit counter, kept together so every call gets a larger stamp,
        // across threads too. A clock that goes backwards or more than 4096 Guids in a millisecond
        // just keep counting forward from the last stamp.
        static std::atomic<std::uint64_t> lastStamp{0};

        const auto now = std::chrono::system_clock::now().time_since_epoch();
        const auto milliseconds = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
        const auto candidate = milliseconds << 12;

        auto previous = lastStamp.load(std::memory_order_relaxed);
        std::uint64_t stamp;
        do{
            stamp = std::max(candidate, previous + 1);
        } while (!lastStamp.compare_exchange_weak(previous, stamp, std::memory_order_relaxed));

        const auto high = ((stamp >> 12) << 16) | 0x7000u | (stamp & 0x0FFFu);                        // ms | version 7 | counter
        const auto low = (ThreadGenerator()() & 0x3FFFFFFFFFFFFFFFull) | 0x8000000000000000ull;        // variant | 62 random bits

        const std::uint64_t words[2] = {std::byteswap(high), std::byteswap(low)};

        Guid result;
        std::memcpy(result._data, words, GUID_SIZE);
        return result;
    }
}
