// Endian tools -----------------------------

#include <Rconfig.h> // for WORDS_BIGENDIAN

// IS_LITTLE_ENDIAN, IS_BIG_ENDIAN
#if defined(WORDS_BIGENDIAN)
#define IS_BIG_ENDIAN
#undef IS_LITTLE_ENDIAN
#else
#define IS_LITTLE_ENDIAN
#undef IS_BIG_ENDIAN
#endif

// The following guarantees declaration of the byte swap functions
// (bswap_16, bswap_32, bswap_64).
// from s2 library port.h

#if defined(_WIN32)

#ifdef __cplusplus
#include <cstdlib>
#else
#include <stdlib.h>
#endif

#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__APPLE__)
// Mac OS X / Darwin features
#include <libkern/OSByteOrder.h>
#define bswap_16(x) OSSwapInt16(x)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)

#elif defined(__GLIBC__) || defined(__BIONIC__) || defined(__ASYLO__)
#include <byteswap.h>  // IWYU pragma: export

#else

#ifdef __cplusplus
#include <cstdint>
#else
#include <stdint.h>
#endif

static inline uint16_t bswap_16(uint16_t x) {
#ifdef __cplusplus
  return static_cast<uint16_t>(((x & 0xFF) << 8) | ((x & 0xFF00) >> 8));
#else
  return (uint16_t)(((x & 0xFF) << 8) | ((x & 0xFF00) >> 8));  // NOLINT
#endif  // __cplusplus
}
#define bswap_16(x) bswap_16(x)
static inline uint32_t bswap_32(uint32_t x) {
  return (((x & 0xFF) << 24) |
          ((x & 0xFF00) << 8) |
          ((x & 0xFF0000) >> 8) |
          ((x & 0xFF000000) >> 24));
}
#define bswap_32(x) bswap_32(x)
static inline uint64_t bswap_64(uint64_t x) {
  return (((x & 0xFFULL) << 56) |
          ((x & 0xFF00ULL) << 40) |
          ((x & 0xFF0000ULL) << 24) |
          ((x & 0xFF000000ULL) << 8) |
          ((x & 0xFF00000000ULL) >> 8) |
          ((x & 0xFF0000000000ULL) >> 24) |
          ((x & 0xFF000000000000ULL) >> 40) |
          ((x & 0xFF00000000000000ULL) >> 56));
}
#define bswap_64(x) bswap_64(x)

#endif
