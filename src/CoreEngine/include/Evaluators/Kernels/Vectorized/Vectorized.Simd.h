#pragma once
#include <immintrin.h>          // AVX2 intrinsics. Build with -mavx2 (GCC/Clang) or /arch:AVX2 (MSVC).
#include <type_traits>
#include "../../../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine::VectorizedKernels{

    // A type takes the SIMD comparison path iff it is a fixed-width integer lane.
    // TinyInt / SmallInt / Int / BigInt qualify; Decimal / Guid / StringValue are
    // structs (non-integral) and fall through to the scalar path.
    template<typename T>
    inline constexpr bool IsSimdComparable = std::is_integral_v<T>;

    // A SIMD lane is a signed, fixed-width integer of 1/2/4/8 bytes. Constrain here
    // rather than in the kernel so a bad instantiation fails at the trait, clearly.
    template<typename T>
    concept SimdLane =
        std::is_same_v<T, TinyInt>
        || std::is_same_v<T, SmallInt>
        || std::is_same_v<T, Int>
        || std::is_same_v<T, BigInt>;

    // One 256-bit AVX2 register's worth of SIMD comparison primitives, generic over
    // lane width. LANES / LANE_MASK derive from sizeof(T); the intrinsic is selected
    // at compile time with `if constexpr (sizeof(T) == N)` -- untaken branches are
    // discarded, so no ill-typed intrinsic call is ever emitted. Every member is a
    // thin wrapper so the kernel body only ever talks to SimdOps<T>, never a raw
    // _mm256_* call.
    template<SimdLane T>
    struct SimdOps{
        static constexpr auto LANES = 32 / sizeof(T); // 256 bits / (8*sizeof)
        static constexpr auto LANE_MASK = // 64-bit shift dodges 1u<<32 UB
            static_cast<UnsignedInt>((UnsignedBigInt{1} << LANES) - 1);

        static __m256i LoadVector(const T* vector){
            return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(vector));
        }

        static __m256i SplatConstant(const T value){
            if constexpr (std::is_same_v<T, TinyInt>)
                return _mm256_set1_epi8(value);
            else if constexpr (std::is_same_v<T, SmallInt>)
                return _mm256_set1_epi16(value);
            else if constexpr (std::is_same_v<T, Int>)
                return _mm256_set1_epi32(value);
            else
                return _mm256_set1_epi64x(value);
        }

        static __m256i Equal(const __m256i lhs, const __m256i rhs){
            if constexpr (std::is_same_v<T, TinyInt>)
                return _mm256_cmpeq_epi8(lhs, rhs);
            else if constexpr (std::is_same_v<T, SmallInt>)
                return _mm256_cmpeq_epi16(lhs, rhs);
            else if constexpr (std::is_same_v<T, Int>)
                return _mm256_cmpeq_epi32(lhs, rhs);
            else
                return _mm256_cmpeq_epi64(lhs, rhs);
        }

        static __m256i GreaterThan(const __m256i lhs, const __m256i rhs){   // signed compare (types are int8..int64)
            if constexpr (std::is_same_v<T, TinyInt>)
                return _mm256_cmpgt_epi8(lhs, rhs);
            else if constexpr (std::is_same_v<T, SmallInt>)
                return _mm256_cmpgt_epi16(lhs, rhs);
            else if constexpr (std::is_same_v<T, Int>)
                return _mm256_cmpgt_epi32(lhs, rhs);
            else
                return _mm256_cmpgt_epi64(lhs, rhs);
        }

        static UnsignedInt MoveMask(const __m256i mask){
            if constexpr (std::is_same_v<T, TinyInt>)
                return static_cast<UnsignedInt>(_mm256_movemask_epi8(mask));            // 32 bits, native
            else if constexpr (std::is_same_v<T, SmallInt>){
                // AVX2 has no 16-bit movemask: saturate-pack to bytes then movemask.
                // packs_epi16 works per 128-bit half -> bytes land [lo|hi|lo|hi];
                // permute4x64 restitches ascending lane order before the movemask.
                const auto packed  = _mm256_packs_epi16(mask, mask);
                const auto ordered = _mm256_permute4x64_epi64(packed, 0xD8);
                return static_cast<UnsignedInt>(_mm256_movemask_epi8(ordered)) & LANE_MASK;
            }
            else if constexpr (std::is_same_v<T, Int>)
                return static_cast<UnsignedInt>(_mm256_movemask_ps(_mm256_castsi256_ps(mask)));  // 8 bits
            else
                return static_cast<UnsignedInt>(_mm256_movemask_pd(_mm256_castsi256_pd(mask)));  // 4 bits
        }
    };

    enum class SimdCmpOp : UnsignedTinyInt{
        Equal,
        NotEqual,
        GreaterThan,
        LessThan,
        GreaterEqual,
        LessEqual
    };

    template <SimdLane T, SimdCmpOp Op>
    UnsignedInt Compare(const __m256i lhs, const __m256i rhs){
        __m256i mask;
        if constexpr (Op == SimdCmpOp::Equal || Op == SimdCmpOp::NotEqual)
            mask = SimdOps<T>::Equal(lhs, rhs);
        else if constexpr (Op == SimdCmpOp::GreaterThan || Op == SimdCmpOp::LessEqual)
            mask = SimdOps<T>::GreaterThan(lhs, rhs);
        else
            mask = SimdOps<T>::GreaterThan(rhs, lhs);

        auto bits = SimdOps<T>::MoveMask(mask);
        if constexpr (Op == SimdCmpOp::NotEqual || Op == SimdCmpOp::LessEqual || Op == SimdCmpOp::GreaterEqual)
            bits = ~bits & SimdOps<T>::LANE_MASK;   // derived-by-negation ops; mask clears high bits

        return bits;
    }
}
