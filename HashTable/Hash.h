#pragma once

#include <stdint.h>

inline uint64_t intHash64(uint64_t x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

inline uint64_t intHashCRC32(uint64_t x)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(-1U, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x);
#endif
}