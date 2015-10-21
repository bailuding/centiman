#ifndef __STORAGE_STORAGE_HASH_NODE_H__
#define __STORAGE_STORAGE_HASH_NODE_H__

#include <util/const.h>
#include <util/type.h>

struct StorageHashNode
{
    Key mKey;
    Value mValues[3];
    Seqnum mSeqnums[3];
    StorageHashNode * mpNext;
    
    StorageHashNode(const Key & key, const Value & value, const Seqnum & seqnum, StorageHashNode * const pNext)
    {
        assert(key.mVal != NULL);
        assert(value.mVal != NULL);
        assert(key.mCnt > 0);
        assert(value.mCnt > 0);
        mKey.copyFrom(key);
        mValues[0].copyFrom(value);
        mSeqnums[0] = seqnum;
        mSeqnums[1] = mSeqnums[2] = Const::SEQNUM_NULL;
        mpNext = pNext;
    }

    /* return 1 if insert successfully */
    int insert(const Value & value, const Seqnum & seqnum)
    {
        assert(value.mCnt > 0);
        int idx = -1;
        /* if a value is not filled */
        for (int i = 0; i < 3; ++i) {
            if (mSeqnums[i] == Const::SEQNUM_NULL) {
                idx = i;
                break;
            }
        }
        /* find the smallest version */
        if (idx == -1) {
            idx = 0;
            for (int i = 1; i < 3; ++i)
                if (mSeqnums[i] < mSeqnums[idx])
                    idx = i;
        }
        /* replace the smallest value */
        assert(value.mCnt > 0);
        if (mSeqnums[idx] == Const::SEQNUM_NULL 
            || seqnum > mSeqnums[idx]) {
            mValues[idx].copyFrom(value);
            mSeqnums[idx] = seqnum;
//            fprintf(stdout, "insert seq %lu %lu %lu %lu\n", seqnum, mSeqnums[0], mSeqnums[1], mSeqnums[2]);
            return 1;
        }
//        fprintf(stdout, "insert fail seq %lu %lu %lu %lu\n", seqnum, mSeqnums[0], mSeqnums[1], mSeqnums[2]);
        return 0;
    }

    /* get the latest version and copy value and seqnum
     * return -1 if there is no value for the key
     */
    int latest(Value * value, Seqnum * seqnum)
    {
        int idx = -1;
        for (int i = 0; i < 3; ++i) {
            if (mSeqnums[i] != Const::SEQNUM_NULL
                && (idx == -1 || mSeqnums[i] > mSeqnums[idx]))
                idx = i;
        }
        if (idx == -1)
            return -1;
        value->copyFrom(mValues[idx]);
        *seqnum = mSeqnums[idx];
        assert(*seqnum != Const::SEQNUM_NULL);
        return 0;
    }

    /* get the latest version that is earlier than readSeqnum
     * copy value and seqnum
     * return -1 if key does not exist
     */
    int latest(Seqnum readSeqnum, Value * value, Seqnum * seqnum)
    {
        int idx = -1;
        int min = -1;
        for (int i = 0; i < 3; ++i) {
            if (mSeqnums[i] <= readSeqnum
                && (idx == -1 || mSeqnums[i] > mSeqnums[idx]))
                idx = i;
            if (mSeqnums[i] != Const::SEQNUM_NULL 
                && (min == -1 || mSeqnums[i] < mSeqnums[min])) {
                min = i;
            }
        }
        if (idx == -1) {
            /* if not found, return the minimal version available */
            value->copyFrom(mValues[min]);
            *seqnum = mSeqnums[min];
            return -1;
        }
        value->copyFrom(mValues[idx]);
        *seqnum = mSeqnums[idx];
        assert(*seqnum != Const::SEQNUM_NULL);
//        fprintf(stdout, "latest readseq %lu getseq %lu %lu %lu %lu\n", readSeqnum, *seqnum, mSeqnums[0], mSeqnums[1], mSeqnums[2]);
        return 0;
    }
}; 

#endif // __STORAGE_STORAGE_HASH_NODE_H__
