#ifndef __UTIL_BYTES_H__
#define __UTIL_BYTES_H__

#include <algorithm>
#include <atomic>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#include <util/random.h>

/**
 * Byte array without worrying about boundaries 
 * The maximum size of a byte array is 32768 bytes
 */
struct Bytes {
    uint8_t * mVal;
    uint16_t mCnt;
    uint16_t mSize;
    Random mRand;
    static Random mSeed;

    Bytes(): mVal(NULL), mCnt(0), mSize(0), mRand(mSeed.rand()) 
    {
  //      mVal = new uint8_t[64 - sizeof mCnt - sizeof mSize - sizeof mRand];
    } 

    Bytes(uint16_t size): mCnt(0), mSize(size), mRand(mSeed.rand())
    {
        mVal = new uint8_t[mSize] ();
    }

    ~Bytes()
    {
        delete[] mVal;
        //   free(mVal);
    }

    void copyFrom(uint8_t * val, uint16_t cnt)
    {
        assert(cnt >= 0);
        if (val == NULL) {
            assert(cnt == 0);
            mCnt = cnt;
            return;
        }
        if (cnt <= mSize) {
            memcpy(mVal, val, cnt);
            mCnt = cnt;
        } else {
            delete[] mVal;
            mVal = new uint8_t[cnt];
            memcpy(mVal, val, cnt);
            mCnt = cnt;
            mSize = cnt;
        }
    }

    void copyFrom(const Bytes & bytes)
    {
        copyFrom(bytes.mVal, bytes.mCnt);
    }

    bool isEqual(const Bytes & rhs) const
    {
        return  mCnt == rhs.mCnt && !memcmp(mVal, rhs.mVal, mCnt);
    }

    void setBytes(int value)
    {
        for (int i = 0; i < mCnt; ++i) {
            mVal[i] = value & 255;
            value >>= 8;
        }
    }

    void getRand(uint16_t cnt, int size)
    {
        if (cnt > mSize) {
            delete[] mVal;
            mVal = new uint8_t[cnt];
            mSize = cnt;
        }
        mCnt = cnt;
        int value = mRand.rand() % size;
        setBytes(value);

        /* std::cout << "bk " << value << " ";
        for (int i = 0; i < mCnt; ++i)
            std::cout << int(mVal[i]) << " ";
        std::cout << "bki " << toInt() << std::endl;
        */

    }
    
    int toInt()
    {
        int val = 0;
        for (int i = mCnt - 1; i >= 0; --i) {
            val <<= 8;
            val = val + mVal[i];
        }
        return val;
    }

    void setSize(uint16_t cnt) 
    {   
        if (cnt > mSize) {
            delete[] mVal;
            mVal = new uint8_t[cnt];
            mSize = cnt;
        }
        mCnt = cnt;
    }

    void print()
    {
        fprintf(stdout, "%d ", mCnt);
        for (int i = 0; i < mCnt; ++i) {
            fprintf(stdout, "%02X", mVal[i]);
        }
        fprintf(stdout, "\n");
    }

    void print(FILE * file)
    {
        fprintf(file, "%d ", mCnt);
        for (int i = 0; i < mCnt; ++i) {
            fprintf(file, "%02X", mVal[i]);
        }
        fprintf(file, "\n");
    }
}; 

#endif // __UTIL_BYTES_H__
