#ifndef __UTIL_INDEX_H__
#define __UTIL_INDEX_H__

#include <util/const.h>
#include <util/debug.h>
#include <util/type.h>
#include "util/hash-config.h"
#include "util/logger.h"

class Index
{
    public:
        static int getStorageIdx(const Key * key)
        {
            int rv = getIdx(key) % Const::STORAGE_NUM;
            assert(rv >= 0);
            return rv;
//            return getIdx(key) % Const::STORAGE_NUM;
        }
        static int getStorageIdx(int idx)
        {
            return idx % Const::STORAGE_NUM;
        }
        static int getStorageHashIdx(const Key * key)
        {
            return getIdx(key) / Const::STORAGE_NUM;
        }

        static int getValidatorId(HashConfig * config, Key * key)
        {
            int hashkey = getIdx(key);
            return config->getId(hashkey);
        }

        static int getValidatorIdx(const Key * key)
        {

#ifdef __UTIL_DEBUG_EVEN_SPLIT__
            return key->mVal[0] % Const::VALIDATOR_NUM;
#endif
            return getIdx(key) % Const::VALIDATOR_NUM;
        }
        static int getValidatorHashIdx(const Key * key) 
        {
            return getIdx(key) / Const::VALIDATOR_NUM;
        }
        static int getValidatorHashIdx(int idx)
        {
            return idx / Const::VALIDATOR_NUM;
        }
        static uint32_t getIdx(const Key * key) 
        {
            uint32_t idx = 0;
            for (int i = key->mCnt - 1; i >= 0; --i) {
                idx = idx * 889 + key->mVal[i];
            }
            return idx;
        }
};

#endif // __UTIL_INDEX_H__
