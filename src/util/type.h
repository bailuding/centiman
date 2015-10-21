#ifndef __UTIL_TYPE_H__
#define __UTIL_TYPE_H__

#include <util/array-bytes.h>
#include <util/bytes.h>

typedef uint64_t    Seqnum;
typedef uint16_t    KeySize;
typedef uint16_t    ValueSize;
typedef uint32_t    XctionId;
typedef uint16_t     XctionSize;

typedef Bytes       Key;
typedef Bytes       Value;

typedef ArrayBytes  ArrayKey;
typedef ArrayBytes  ArrayValue;

#endif 
