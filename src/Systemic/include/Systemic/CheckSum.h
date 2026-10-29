#pragma once

#include <nmmintrin.h>   // SSE4.2: _mm_crc32_*
#include <wmmintrin.h>   // PCLMUL: _mm_clmulepi64_si128
#include <immintrin.h>   // AVX-512 + VPCLMULQDQ
#include <cstring>
#include <cstdint>

#include <Systemic/DataTypes/DataTypes.h>

namespace CheckSum {

    namespace Detail {
        constexpr UnsignedInt Poly = 0x82F63B78u;   // CRC32C, reflected

        [[nodiscard]] inline UnsignedBigInt Load64(const object_t* p) noexcept {
            UnsignedBigInt v;
            std::memcpy(&v, p, 8);
            return v;
        }

        // a * b mod P, both in reflected form
        [[nodiscard]] constexpr UnsignedInt MulModP(const UnsignedInt a, UnsignedInt b) noexcept {
            UnsignedInt r = 0;
            for (int i = 0; i < 32; ++i) {
                if (a & (0x80000000u >> i)) r ^= b;
                b = (b >> 1) ^ ((b & 1u) ? Poly : 0u);
            }
            return r;
        }

        // x^n mod P, reflected
        [[nodiscard]] constexpr UnsignedInt XPowModP(UnsignedBigInt n) noexcept {
            UnsignedInt result = 0x80000000u;   // x^0
            UnsignedInt base   = 0x40000000u;   // x^1
            while (n) {
                if (n & 1u) result = MulModP(result, base);
                base = MulModP(base, base);
                n >>= 1;
            }
            return result;
        }

        // Multiplier that advances a CRC past `bytes` zero bytes
        [[nodiscard]] constexpr UnsignedInt ShiftConstant(const UnsignedBigInt bytes) noexcept {
            return XPowModP(8 * bytes - 33);
        }

        // crc * x^(8*bytes) mod P, using one carry-less multiply + one crc32
        [[nodiscard]] inline UnsignedInt Shift(const UnsignedInt crc, const UnsignedInt k) noexcept {
            const __m128i product = _mm_clmulepi64_si128(_mm_cvtsi32_si128(static_cast<int>(crc)),
                                                         _mm_cvtsi32_si128(static_cast<int>(k)), 0x00);
            return static_cast<UnsignedInt>(
                _mm_crc32_u64(0, static_cast<UnsignedBigInt>(_mm_cvtsi128_si64(product))));
        }
    }

    // Three independent CRC chains over three thirds of the buffer, merged at the end.
    // The crc32 instruction has ~3 cycles latency but 1/cycle throughput, so three
    // chains keep the unit busy: ~3x the speed of a single chain.
    template<UnsignedInt Size>
    requires (Size % 8 == 0 && Size >= 24)
    [[nodiscard]] inline UnsignedInt Crc32(const object_t* data) noexcept {
        constexpr UnsignedInt Lane = (Size / 24) * 8;        // bytes per stream, multiple of 8
        constexpr UnsignedInt Tail = Size - 3 * Lane;        // 0, 8 or 16 bytes
        constexpr UnsignedInt K1 = Detail::ShiftConstant(Lane);
        constexpr UnsignedInt K2 = Detail::ShiftConstant(2 * Lane);

        const object_t* a = data;
        const object_t* b = data + Lane;
        const object_t* c = data + 2 * Lane;

        UnsignedBigInt crcA = 0xFFFFFFFFu;
        UnsignedBigInt crcB = 0;
        UnsignedBigInt crcC = 0;

        for (UnsignedInt i = 0; i < Lane; i += 8) {
            crcA = _mm_crc32_u64(crcA, Detail::Load64(a + i));
            crcB = _mm_crc32_u64(crcB, Detail::Load64(b + i));
            crcC = _mm_crc32_u64(crcC, Detail::Load64(c + i));
        }

        UnsignedBigInt crc = Detail::Shift(static_cast<UnsignedInt>(crcA), K2)
                          ^ Detail::Shift(static_cast<UnsignedInt>(crcB), K1)
                          ^ static_cast<UnsignedInt>(crcC);

        const object_t* tail = data + 3 * Lane;
        for (UnsignedInt i = 0; i < Tail; i += 8)
            crc = _mm_crc32_u64(crc, Detail::Load64(tail + i));

        return static_cast<UnsignedInt>(~static_cast<UnsignedInt>(crc));
    }

    // Runtime size, single chain: for anything that isn't a fixed block
    [[nodiscard]] inline UnsignedInt Crc32(const object_t* data, UnsignedInt size) noexcept {
        UnsignedBigInt crc = 0xFFFFFFFFu;

        while (size >= 8) {
            crc = _mm_crc32_u64(crc, Detail::Load64(data));
            data += 8;
            size -= 8;
        }

        auto crc32 = static_cast<UnsignedInt>(crc);
        if (size >= 4) {
            UnsignedInt chunk;
            std::memcpy(&chunk, data, 4);
            crc32 = _mm_crc32_u32(crc32, chunk);
            data += 4;
            size -= 4;
        }
        if (size >= 2) {
            std::uint16_t chunk;
            std::memcpy(&chunk, data, 2);
            crc32 = _mm_crc32_u16(crc32, chunk);
            data += 2;
            size -= 2;
        }
        if (size == 1)
            crc32 = _mm_crc32_u8(crc32, *data);

        return static_cast<UnsignedInt>(~crc32);
    }

    // -------------------------------------------------------------------------
    // AVX-512 + VPCLMULQDQ: carry-less multiply folding, 256 bytes per iteration.
    // Needs AVX512F and VPCLMULQDQ (Intel Ice Lake and newer, AMD Zen 4 and newer).
    // Compiled with a target attribute, so the rest of the program does not need
    // -mavx512f; only call it after checking the CPU (see Crc32Fast below).
    // -------------------------------------------------------------------------
#if defined(__GNUC__) || defined(__clang__)
    #define CHECKSUM_AVX512_TARGET __attribute__((target("avx512f,vpclmulqdq,pclmul,sse4.2")))
#else
    #define CHECKSUM_AVX512_TARGET
#endif

    namespace Detail {
        // Constants that move a 128-bit block `bits` further along the message.
        // `hi` multiplies the first 8 bytes of the block, `lo` the last 8 bytes.
        struct FoldPair { UnsignedBigInt hi, lo; };

        [[nodiscard]] constexpr FoldPair FoldConstants(const UnsignedBigInt bits) noexcept {
            return { XPowModP(bits + 64 - 33), XPowModP(bits - 33) };
        }

        [[nodiscard]] CHECKSUM_AVX512_TARGET inline __m512i Broadcast(FoldPair k) noexcept {
            const auto hi = static_cast<long long>(k.hi);
            const auto lo = static_cast<long long>(k.lo);
            return _mm512_set_epi64(lo, hi, lo, hi, lo, hi, lo, hi);
        }

        // (x folded forward) ^ next, on four 128-bit lanes at once
        [[nodiscard]] CHECKSUM_AVX512_TARGET inline __m512i Fold512(__m512i x, __m512i k, __m512i next) noexcept {
            return _mm512_ternarylogic_epi64(_mm512_clmulepi64_epi128(x, k, 0x00),
                                             _mm512_clmulepi64_epi128(x, k, 0x11),
                                             next, 0x96);   // a ^ b ^ c
        }

        [[nodiscard]] CHECKSUM_AVX512_TARGET inline __m128i Fold128(__m128i x, const FoldPair k, const __m128i next) noexcept {
            const __m128i kv = _mm_set_epi64x(static_cast<long long>(k.lo), static_cast<long long>(k.hi));
            return _mm_xor_si128(_mm_xor_si128(_mm_clmulepi64_si128(x, kv, 0x00),
                                               _mm_clmulepi64_si128(x, kv, 0x11)),
                                 next);
        }
    }

    template<UnsignedInt Size>
    requires (Size % 256 == 0 && Size >= 256)
    [[nodiscard]] CHECKSUM_AVX512_TARGET inline UnsignedInt Crc32Avx512(const object_t* data) noexcept {
        constexpr Detail::FoldPair K2048 = Detail::FoldConstants(2048);
        constexpr Detail::FoldPair K512  = Detail::FoldConstants(512);
        constexpr Detail::FoldPair K384  = Detail::FoldConstants(384);
        constexpr Detail::FoldPair K256  = Detail::FoldConstants(256);
        constexpr Detail::FoldPair K128  = Detail::FoldConstants(128);

        // Four 64-byte accumulators = 256 bytes in flight
        __m512i x0 = _mm512_loadu_si512(data);
        __m512i x1 = _mm512_loadu_si512(data + 64);
        __m512i x2 = _mm512_loadu_si512(data + 128);
        __m512i x3 = _mm512_loadu_si512(data + 192);

        // Initial CRC value 0xFFFFFFFF goes into the first 4 bytes
        x0 = _mm512_xor_si512(x0, _mm512_set_epi64(0, 0, 0, 0, 0, 0, 0, 0xFFFFFFFFll));

        const __m512i k2048 = Detail::Broadcast(K2048);
        for (UnsignedInt i = 256; i < Size; i += 256) {
            x0 = Detail::Fold512(x0, k2048, _mm512_loadu_si512(data + i));
            x1 = Detail::Fold512(x1, k2048, _mm512_loadu_si512(data + i + 64));
            x2 = Detail::Fold512(x2, k2048, _mm512_loadu_si512(data + i + 128));
            x3 = Detail::Fold512(x3, k2048, _mm512_loadu_si512(data + i + 192));
        }

        // 4 x 512 bits -> 512 bits
        const __m512i k512 = Detail::Broadcast(K512);
        x1 = Detail::Fold512(x0, k512, x1);
        x2 = Detail::Fold512(x1, k512, x2);
        x3 = Detail::Fold512(x2, k512, x3);

        // 4 x 128 bits -> 128 bits
        const __m128i a = _mm512_extracti32x4_epi32(x3, 0);
        const __m128i b = _mm512_extracti32x4_epi32(x3, 1);
        const __m128i c = _mm512_extracti32x4_epi32(x3, 2);
        const __m128i d = _mm512_extracti32x4_epi32(x3, 3);
        __m128i r = Detail::Fold128(a, K384, d);
        r = Detail::Fold128(b, K256, r);
        r = Detail::Fold128(c, K128, r);

        // 128 bits -> 32-bit CRC with the hardware crc32 instruction
        UnsignedBigInt crc = _mm_crc32_u64(0, static_cast<UnsignedBigInt>(_mm_cvtsi128_si64(r)));
        crc = _mm_crc32_u64(crc, static_cast<UnsignedBigInt>(_mm_extract_epi64(r, 1)));
        return static_cast<UnsignedInt>(~static_cast<UnsignedInt>(crc));
    }

    // Picks the AVX-512 version when the CPU has it, otherwise the 3-chain version.
    template<UnsignedInt Size>
    requires (Size % 256 == 0 && Size >= 256)
    [[nodiscard]] inline UnsignedInt Crc32Fast(const object_t* data) noexcept {
#if defined(__GNUC__) || defined(__clang__)
        static const bool hasAvx512 = __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("vpclmulqdq");
        if (hasAvx512)
            return Crc32Avx512<Size>(data);
#endif
        return Crc32<Size>(data);
    }

#undef CHECKSUM_AVX512_TARGET

}
